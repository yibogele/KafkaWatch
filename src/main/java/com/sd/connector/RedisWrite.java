package com.sd.connector;

import redis.clients.jedis.Jedis;

/**
 * Author: Will Fan
 * Created: 2019/10/11 10:38
 * Description:
 */
public class RedisWrite {
    public static void main(String[] args) {
        Jedis jedis = new Jedis(CommonDefs.REDIS_HOST);
//        jedis.hset("")
        jedis.select(2);
        jedis.hset("test:123", "name", "teset");
        jedis.hset("test:123", "age", "234");
        jedis.close();
    }
}
