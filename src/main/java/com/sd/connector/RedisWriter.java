package com.sd.connector;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * Author: Will Fan
 * Created: 2019/10/11 9:07
 * Description:
 */
public class RedisWriter extends RichSinkFunction<Tuple6<String, String, String, Long, Long, Long>> {
    //    private JedisPool jedisPool;
    private Jedis redisConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisConn = new Jedis(CommonDefs.REDIS_HOST, 6379, 100000);
        redisConn.select(CommonDefs.REDIS_DB);
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (redisConn != null)
            redisConn.close();
    }

    @Override
    public void invoke(Tuple6<String, String, String, Long, Long, Long> value, Context context) {
        String key = CommonDefs.REDIS_KEY_PREFIX + value.f1 + ":" + value.f0 + ":" + value.f2;
        redisConn.hset(key, "devId", value.f0);
        redisConn.hset(key, "productKey", value.f1);
        redisConn.hset(key, "dataType", value.f2);
        redisConn.hset(key, "startETime", value.f3.toString());
        redisConn.hset(key, "processTime", value.f4.toString());
        redisConn.hset(key, "count", value.f5.toString());

        redisConn.expire(key, CommonDefs.SHORT_TIME);
    }


}
