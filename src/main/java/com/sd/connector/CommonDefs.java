package com.sd.connector;

/**
 * Author: Will Fan
 * Created: 2019/10/10 11:27
 * Description:
 */
public final class CommonDefs {
    public static final String DEVICE_DATA_TOPIC = "xhw_device_data_topic";

    public static final String AD_DATA_TOPIC = "person_info";

    public static final String SINK_TOPIC = "stat_data_topic";
    public static final String SHADOW_TOPIC = "shadow_data_topic12";
    public static final String SOURCE_BROKER_LIST = "192.168.1.6:9092";
    public static final String SOURCE_ZOOKEEPER_LIST = "192.168.1.3:2181";
    public static final String SINK_BROKER_LIST = "192.168.1.6:9092";

    static final String REDIS_HOST = "192.168.10.245";
    static final int REDIS_DB = 0;
    static final String REDIS_KEY_PREFIX = "10min:";
    static final String REDIS_KEY_PREFIX2 = "dayNow:";
    static final String REDIS_KEY_WARN = "Online:";

    static final String PGSQL_HOST = "jdbc:postgresql://192.168.10.22:5432/iot";
    static final String PGSQL_USER = "postgres";
    static final String PGSQL_PWD = "postgres";

    private static final int MS = 1;
    public static final int SECOND = 1000 * MS;
    public static final int MINUTE = 60 * SECOND;
    public static final int HOUR = 60 * MINUTE;
    public static final int DAY = 24 * HOUR;

//    public static final int SHORT_TIME = 10 * MINUTE;
//    public static final int LONG_TIME = DAY;
}
