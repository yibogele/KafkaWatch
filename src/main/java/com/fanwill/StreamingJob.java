/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fanwill;

import com.fanwill.codec.IotStringSchema;
import com.fanwill.connector.*;
import com.fanwill.model.InData;
import com.fanwill.model.OutData;
import com.fanwill.op.DevCountAgg;
import com.fanwill.op.ProductCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;

import java.util.*;

/**
 * Author: Will Fan
 * Created: 2019/10/11 9:00
 * Description:
 */
public class StreamingJob {


    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.enableCheckpointing(60000);

        // config
        ParameterTool parameters = ParameterTool.fromPropertiesFile(
                StreamingJob.class.getResourceAsStream("/my.properties"));
        env.getConfig().setGlobalJobParameters(parameters);

        int parallelism = parameters.getInt("op.parallelism", 2);


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",
                parameters.get("kafka.bootstrap.servers", CommonDefs.SOURCE_BROKER_LIST));
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect",
                parameters.get("kafka.zookeeper.connect", CommonDefs.SOURCE_ZOOKEEPER_LIST));
        properties.setProperty("group.id",
                parameters.get("kafka.source.groupid", "group_sdt_java_send_stat1"));

        List<String> topics = new ArrayList<>();
//        topics.add(CommonDefs.AD_DATA_TOPIC);
//        topics.add(CommonDefs.DEVICE_DATA_TOPIC);
        topics.add(parameters.get("kafka.source.topic", CommonDefs.SHADOW_TOPIC));

        // devidStr,
        // devTypeStr,
        // messageType,
        // messageTime
        DataStream<InData> dataStream =
                env.addSource(new FlinkKafkaConsumer08<>(topics, new IotStringSchema(), properties))
//                .setParallelism(parallelism)
                ;


        Set<SinkerOption> set = new HashSet<>();
        set.add(SinkerOption.Redis);
        set.add(SinkerOption.Redis2);
        processDS(dataStream,parallelism,
                Time.minutes(10), Time.minutes(0), set, false);


        set.clear();
        set.add(SinkerOption.PgSql);
//        processDS(dataStream, Time.days(1), Time.hours(16) , set, false);
//        processDS(dataStream, Time.hours(8), Time.hours(0), set, false);
        processDS(dataStream, parallelism,
                Time.hours(12), Time.hours(4), set, false);
//        processDS(dataStream, Time.minutes(10), Time.minutes(0), set, false);

        processWarn(dataStream, parallelism,
                Time.seconds(parameters.getInt("iot.product.off.length")), false);

        // execute program
        env.execute("IotStat-hi");
    }

    private static void processDS(DataStream<InData> ds,
                                  int p,
                                  Time size, Time offset,
                                  Set<SinkerOption> sinkerOptions,
                                  boolean debugToConsole) {


        // devidStr,messageType,stime,ptime,count
        DataStream<OutData> countStream = ds
                .keyBy(0, 2)
                .window(TumblingProcessingTimeWindows.of(size, offset))
                .aggregate(new DevCountAgg()).setParallelism(p);

        // print
        if (debugToConsole)
            countStream.print("Kafka数据统计");

        // redis
        if (sinkerOptions.contains(SinkerOption.Redis))
            countStream.addSink(new RedisWriter()).name("Redis");

        // pgsql
        if (sinkerOptions.contains(SinkerOption.PgSql))
            countStream.addSink(new PgsqlWriter()).name("Pgsql");

        // kafka
        if (sinkerOptions.contains(SinkerOption.Kafka)) {
            countStream
                    .map((MapFunction<OutData, String>) OutData::toString)
                    .addSink(new FlinkKafkaProducer08<>(
                            CommonDefs.SINK_BROKER_LIST,
                            CommonDefs.SINK_TOPIC,
                            new SimpleStringSchema()));
        }

        // redis2
        if (sinkerOptions.contains(SinkerOption.Redis2))
            countStream.addSink(new RedisWriter2()).name("redis2");
    }

    private static void processWarn(
            DataStream<InData> ds,int p,
            Time size,
            boolean debugToConsole
    ) {
        DataStream<Tuple2<String, Long>> dataStream = ds
                .keyBy(1)
                .window(TumblingProcessingTimeWindows.of(size))
                .aggregate(new ProductCounter()).setParallelism(p);

        if (debugToConsole)
            dataStream.print("产品:");

        dataStream.addSink(new RedisWarnWriter()).name("redis warn");
    }


}
