package com.fm.Tools.Agg.Pay;

import com.fm.ODS.Main.TopNTypeMainData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PayStatusAgg implements AggregateFunction<TopNTypeMainData, Map<String, String>, Map<String, String>> {

    @Override
    public Map<String, String> createAccumulator() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, String> add(TopNTypeMainData value, Map<String, String> acc) {
        String orderIdStr = value.getOrderId();
        String payStatus = value.getMessage();
        acc.put(orderIdStr, payStatus);
        return acc;
    }

    @Override
    public Map<String, String> getResult(Map<String, String> accumulator) {
        return accumulator;
    }

    @Override
    public Map<String, String> merge(Map<String, String> acc1, Map<String, String> acc2) {
        acc1.putAll(acc2);
        return acc1;
    }

}
