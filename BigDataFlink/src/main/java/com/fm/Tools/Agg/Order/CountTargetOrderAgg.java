package com.fm.Tools.Agg.Order;

import com.fm.ADS.TopNData;
import org.apache.flink.api.common.functions.AggregateFunction;

public class CountTargetOrderAgg implements AggregateFunction<TopNData, TopNData, TopNData> {

    @Override
    public TopNData createAccumulator() {
        return null;
    }

    @Override
    public TopNData add(TopNData topNData, TopNData topNData2) {
        return null;
    }

    @Override
    public TopNData getResult(TopNData topNData) {
        return null;
    }

    @Override
    public TopNData merge(TopNData topNData, TopNData acc1) {
        return null;
    }
}
