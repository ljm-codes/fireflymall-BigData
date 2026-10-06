package com.fm.Tools.Agg;


import com.fm.ADS.TopNData;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountAgg implements AggregateFunction<TopNData, TopNData, TopNData> {

    @Override
    public TopNData createAccumulator() {
        return new TopNData();
    }

    @Override
    public TopNData add(TopNData value, TopNData accumulator) {
        value.setCount(accumulator.getCount() + value.getCount());
        return value;
    }

    @Override
    public TopNData getResult(TopNData accumulator) {
        return accumulator;
    }

    @Override
    public TopNData merge(TopNData a, TopNData b) {
        a.setCount(a.getCount() + b.getCount());
        return a;
    }

}
