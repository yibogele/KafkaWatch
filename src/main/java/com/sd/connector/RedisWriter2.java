package com.sd.connector;

import com.sd.model.OutData;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.Calendar;


/**
 * Author: Will Fan
 * Created: 2019/10/14 16:46
 * Description:
 */
public class RedisWriter2 extends RichSinkFunction<OutData> {

    private Jedis redisConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool gConf = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        redisConn = new Jedis(gConf.get("redis.host", CommonDefs.REDIS_HOST),
                gConf.getInt("redis.port", 6379),
                gConf.getInt("redis.connect.timeout", 100000));
        redisConn.select(gConf.getInt("redis.db", CommonDefs.REDIS_DB));
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (redisConn != null)
            redisConn.close();
    }

    @Override
    public void invoke(OutData value, Context context) {
        String key = CommonDefs.REDIS_KEY_PREFIX2 + value.f1 + ":" + value.f0;
        redisConn.hset(key, "devId", value.f0);

        redisConn.hset(key, "processTime", value.f4.toString());

        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(value.f4);
        int hour = c.get(Calendar.HOUR);
        int minute = c.get(Calendar.MINUTE);

        if (hour == 0 && minute < 10) {
            redisConn.hset(key, "count", value.f5.toString());
        } else {

            long count = 0;
            try {
                count = Long.parseLong(redisConn.hget(key, "count"));
            } catch (NumberFormatException ignored) {
            }

            redisConn.hset(key, "count", String.valueOf(value.f5 + count));
        }

        redisConn.expire(key, 24*60*60);
    }


}
