package com.sd.model;


import org.apache.flink.api.java.tuple.Tuple7;

/**
 * Author: Will Fan
 * Created: 2019/10/16 8:44
 * Description:
 */
@SuppressWarnings("unchecked")
public class OutData extends Tuple7<String, String, String, Long, Long, Long, Long> {
    public OutData() {
        super();
    }


    public OutData(String value0, String value1, String value2, Long value3, Long value4, Long value5, Long value6) {
        super(value0, value1, value2, value3, value4, value5, value6);
    }
}
