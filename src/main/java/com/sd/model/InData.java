package com.sd.model;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Author: Will Fan
 * Created: 2019/10/16 8:43
 * Description:
 */
@SuppressWarnings("unchecked")
public class InData extends Tuple5<String, String, String, Long, Long> {
    public InData(String value0, String value1, String value2, Long value3, Long value4) {
        super(value0, value1, value2, value3, value4);
    }

    public InData() {
        super();
    }
}
