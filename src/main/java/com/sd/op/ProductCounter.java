package com.sd.op;

import com.sd.model.InData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Author: Will Fan
 * Created: 2019/11/13 9:10
 * Description:
 */
class ACCCount{
    String productKey = null;
    long count = 0;
}

public class ProductCounter implements AggregateFunction<InData, ACCCount, Tuple2<String, Long>> {

    @Override
    public ACCCount createAccumulator() {
        return new ACCCount();
    }

    @Override
    public ACCCount add(InData value, ACCCount accumulator) {
        if (accumulator.productKey == null) accumulator.productKey = value.f1;
        accumulator.count++;
        return accumulator;
    }

    @Override
    public Tuple2<String, Long> getResult(ACCCount accumulator) {
        return new Tuple2<>(accumulator.productKey, accumulator.count);
    }

    @Override
    public ACCCount merge(ACCCount a, ACCCount b) {
        if (a.productKey.equals(b.productKey)) {
            a.count += b.count;
            return a;
        }
        return null;
    }
}
