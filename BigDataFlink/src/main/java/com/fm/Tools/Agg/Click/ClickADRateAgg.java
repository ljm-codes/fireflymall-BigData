package com.fm.Tools.Agg.Click;

import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.Main.TopNTypeMainData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClickADRateAgg implements AggregateFunction<TopNTypeMainData, Map<String, Long>, Double> {
    @Override
    public Map<String, Long> createAccumulator() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, Long> add(TopNTypeMainData value, Map<String, Long> acc) {
        List<CommodityRankData> rankData = value.getData();

        for (CommodityRankData data : rankData) {
            String isAD = data.getIsAD() == 1 ? "AD" : "NOT_AD";
            if (acc.containsKey(isAD)) {
                acc.put(isAD, acc.get(isAD) + 1);
            } else {
                acc.put(isAD, 1L);
            }
        }

        return acc;
    }

    @Override
    public Double getResult(Map<String, Long> acc) {
        Long clickNum = acc.getOrDefault("NOT_AD", 0L);
        Long adNum = acc.getOrDefault("AD", 0L);
        return (double) (adNum / (clickNum + adNum));
    }

    @Override
    public Map<String, Long> merge(Map<String, Long> acc1, Map<String, Long> acc2) {
        acc1.putAll(acc2);
        return acc1;
    }
}
