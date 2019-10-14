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

package com.sd;

import com.sd.codec.IotStringSchema;
import com.sd.connector.CommonDefs;
import com.sd.connector.PgsqlWriter;
import com.sd.connector.RedisWriter;
import com.sd.connector.SinkerOption;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;

import java.util.*;


public class StreamingJob {


    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.6:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "192.168.1.3:2181");
        properties.setProperty("group.id", "group_sdt_java_send_stat");

        List<String> topics = new ArrayList<>();
//        topics.add(CommonDefs.AD_DATA_TOPIC);
//        topics.add(CommonDefs.DEVICE_DATA_TOPIC);
        topics.add(CommonDefs.SHADOW_TOPIC);

        // devidStr,
        // devTypeStr,
        // messageType,
        // messageTime
        DataStream<Tuple4<String, String, String, Long>> dataStream =
                env.addSource(new FlinkKafkaConsumer08<>(topics, new IotStringSchema(), properties));

        Set<SinkerOption> set = new HashSet<>();
        set.add(SinkerOption.Redis);
        processWindow(dataStream, Time.minutes(CommonDefs.SHORT_TIME/CommonDefs.MINUTE), set, true);

        set.clear();
        set.add(SinkerOption.PgSql);
        processWindow(dataStream, Time.days(CommonDefs.LONG_TIME/CommonDefs.DAY), set, false);


        // execute program
        env.execute("Iot Data Watcher");
    }

    private static void processWindow(DataStream<Tuple4<String, String, String, Long>> ds,
                                      Time size,
                                      Set<SinkerOption> sinkerOptions,
                                      boolean debugToConsole) {
        class MyCounter {
            String deviceId="", productKey="", dataType="";
            long sTime = 0, pTime = 0;
            long count = 0;
        }

        // devidStr,messageType,stime,ptime,count
        DataStream<Tuple6<String, String, String, Long, Long, Long>> countStream = ds
                .keyBy(0, 2)
                .window(TumblingProcessingTimeWindows.of(size))
                .aggregate(new AggregateFunction<Tuple4<String, String, String, Long>, MyCounter,
                        Tuple6<String, String, String, Long, Long, Long>>() {

                    @Override
                    public MyCounter createAccumulator() {
                        return new MyCounter();
                    }

                    @Override
                    public MyCounter add(Tuple4<String, String, String, Long> value, MyCounter accumulator) {
                        if (accumulator.deviceId.isEmpty()) accumulator.deviceId = value.f0;
                        if (accumulator.productKey.isEmpty()) accumulator.productKey = value.f1;
                        if (accumulator.dataType.isEmpty()) accumulator.dataType = value.f2;
                        // hoping that the OS which this task runs on has same time with others
                        if (accumulator.pTime == 0) accumulator.pTime = System.currentTimeMillis();
                        accumulator.sTime = value.f3;
                        accumulator.count++;
                        return accumulator;
                    }

                    @Override
                    public Tuple6<String, String, String, Long, Long, Long> getResult(MyCounter accumulator) {
                        return new Tuple6<>(accumulator.deviceId, accumulator.productKey,
                                accumulator.dataType, accumulator.sTime,
                                accumulator.pTime, accumulator.count);
                    }

                    @Override
                    public MyCounter merge(MyCounter a, MyCounter b) {
                        return null;
                    }
                });

        // print
        if (debugToConsole)
            countStream.print("Kafka数据统计");

        // redis
        if (sinkerOptions.contains(SinkerOption.Redis))
            countStream.addSink(new RedisWriter());

        // pgsql
        if (sinkerOptions.contains(SinkerOption.PgSql))
            countStream.addSink(new PgsqlWriter());

        // kafka
        if (sinkerOptions.contains(SinkerOption.Kafka)) {
            countStream
                    .map((MapFunction<Tuple6<String, String, String, Long, Long, Long>, String>) Tuple6::toString)
                    .addSink(new FlinkKafkaProducer08<>(
                            "192.168.1.6:9092",
                            CommonDefs.SINK_TOPIC,
                            new SimpleStringSchema()));
        }
    }
}
