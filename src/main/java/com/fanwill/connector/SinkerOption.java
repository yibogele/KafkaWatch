package com.fanwill.connector;

/**
 * Author: Will Fan
 * Created: 2019/10/11 13:01
 * Description:
 */
public enum SinkerOption {
    Redis("redis", 1),
    PgSql("pgsql", 2),
    Kafka("kafka", 4),
    Redis2("redis2", 8);

    private String name;
    private int value;

    private SinkerOption(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public int getValue() {
        return value;
    }


    @Override
    public String toString() {
        return name;
    }
}
