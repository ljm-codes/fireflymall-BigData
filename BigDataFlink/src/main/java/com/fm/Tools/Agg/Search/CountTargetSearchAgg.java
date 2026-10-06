package com.fm.Tools.Agg.Search;

import com.fm.ODS.Main.CommodityRankData;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class CountTargetSearchAgg implements AggregateFunction<Tuple3<String, List<CommodityRankData>, Integer>, Tuple3<String, List<CommodityRankData>, Integer>, Tuple3<String, List<CommodityRankData>, Integer>> {


    @Override
    public Tuple3<String, List<CommodityRankData>, Integer> createAccumulator() {
        return new Tuple3<>("", new ArrayList<>(), 0);
    }

    @Override
    public Tuple3<String, List<CommodityRankData>, Integer> add(Tuple3<String, List<CommodityRankData>, Integer> value, Tuple3<String, List<CommodityRankData>, Integer> acc) {
        System.out.println(value.f0 + ":" + acc.f0 + ":" + acc.f1);
        return new Tuple3<>(value.f0, value.f1, acc.f2 + value.f2);
    }

    @Override
    public Tuple3<String, List<CommodityRankData>, Integer> getResult(Tuple3<String, List<CommodityRankData>, Integer> acc) {
        return acc;
    }

    @Override
    public Tuple3<String, List<CommodityRankData>, Integer> merge(Tuple3<String, List<CommodityRankData>, Integer> acc1, Tuple3<String, List<CommodityRankData>, Integer> acc2) {
        return new Tuple3<>(acc1.f0, acc1.f1, acc1.f2 + acc2.f2);
    }
}
