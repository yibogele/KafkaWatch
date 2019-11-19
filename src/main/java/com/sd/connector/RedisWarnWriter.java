package com.sd.connector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: Will Fan
 * Created: 2019/11/13 9:17
 * Description:
 */
public class RedisWarnWriter extends RichSinkFunction<Tuple2<String, Long>> {
    private Jedis redisConn;
    private int expire;
    private Map<String, String> devMap;

    private void initDevNames(){
        devMap = new HashMap<>();
        devMap.put("c9XeFSPGmbo", "爱动手环"  );
        devMap.put("WxkJfmutLLs", "老年机"    );
        devMap.put("0MRyfGQQw65", "对讲机"    );
        devMap.put("0ICHDc1DqJx", "GPS定位仪" );
        devMap.put("JiRTujzn3XH", "卡片机"    );
        devMap.put("X7mOenlFCE4", "桑德盒子"  );
        devMap.put("3eERaSscrDe", "垃圾分类桶");
        devMap.put("tWXajW6RJXO", "厨余称"    );
        devMap.put("x8RxOwDBoVD", "柠檬科技"  );
        devMap.put("bQShjRwfNIA", "无源定位器");
        devMap.put("4X8AYnmITVU", "凯昆盒子"  );
        devMap.put("RytndLE1QkN", "光伏"      );
        devMap.put("PAWwRnJkvai", "地埋桶"    );
        devMap.put("ApL7Bp4nFkB", "凯昆APP"   );
        devMap.put("VTQFnEOVwzZ", "地秤"      );
        devMap.put("77jT6Aa4GeZ", "四分类桶"  );
        devMap.put("jVcnIMCb3Xf", "无线充电桩");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool gConf = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        redisConn = new Jedis(gConf.get("redis.host", CommonDefs.REDIS_HOST),
                gConf.getInt("redis.port", 6379),
                gConf.getInt("redis.connect.timeout", 100000));
        redisConn.select(gConf.getInt("redis.db", CommonDefs.REDIS_DB));

        expire = gConf.getInt("iot.ds.fault.length", 600);
        System.out.println("redis expire = " + expire + "s");

        initDevNames();
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (redisConn != null)
            redisConn.close();
    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) {
        String key = CommonDefs.REDIS_KEY_WARN + value.f0;

        redisConn.hset(key, "productKey", value.f0);
        redisConn.hset(key, "event count", value.f1.toString());
        redisConn.hset(key, "product name", devMap.get(value.f0));
        redisConn.hset(key, "lastUpdate", String.valueOf(System.currentTimeMillis()));

        redisConn.expire(key, expire);
//        System.out.println(expire);
    }
}
