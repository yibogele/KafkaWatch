package com.fanwill.op;

import com.fanwill.model.InData;
import com.fanwill.model.OutData;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Author: Will Fan
 * Created: 2019/11/13 9:08
 * Description:
 */
class MyCounter {
    String deviceId = "", productKey = "", dataType = "";
    long sTime = 0, pTime = 0;
    long count = 0;
    long traffic = 0;
}

public class DevCountAgg implements AggregateFunction<InData, MyCounter,
        OutData> {

    @Override
    public MyCounter createAccumulator() {
        return new MyCounter();
    }

    @Override
    public MyCounter add(InData value, MyCounter accumulator) {
        if (accumulator.deviceId.isEmpty()) accumulator.deviceId = value.f0;
        if (accumulator.productKey.isEmpty()) accumulator.productKey = value.f1;
        if (accumulator.dataType.isEmpty()) accumulator.dataType = value.f2;
        // hoping that the OS which this task runs on has same time with others
        accumulator.pTime = System.currentTimeMillis();
        if (accumulator.sTime == 0) accumulator.sTime = value.f3;
        accumulator.count++;
        accumulator.traffic += value.f4;
        return accumulator;
    }

    @Override
    public OutData getResult(MyCounter accumulator) {
        return new OutData(accumulator.deviceId, accumulator.productKey,
                accumulator.dataType, accumulator.sTime,
                accumulator.pTime, accumulator.count, accumulator.traffic);
    }

    @Override
    public MyCounter merge(MyCounter a, MyCounter b) {
//        MyCounter mc = new MyCounter();
//        mc.deviceId = a.deviceId;
//        mc.productKey = a.productKey;
//        mc.dataType = a.dataType;
//        mc.sTime = a.sTime;
//        mc.pTime = System.currentTimeMillis();
//        mc.count = a.count + b.count;
//        mc.traffic = a.traffic + b.traffic;
//        return mc;
          a.count += b.count;
          a.traffic += b.traffic;
          a.pTime = System.currentTimeMillis();
          return a;
    }
}