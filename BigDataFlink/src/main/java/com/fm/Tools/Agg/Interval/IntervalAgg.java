package com.fm.Tools.Agg.Interval;

import com.fm.ODS.DWM.BehaviorAnalysisResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class IntervalAgg implements AggregateFunction<BehaviorAnalysisResult, Tuple2<Double, Long>, Double> {

    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return new Tuple2<>(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(BehaviorAnalysisResult result, Tuple2<Double, Long> acc) {
        return new Tuple2<>(
                acc.f0 + result.getValue(),
                acc.f1 + 1L
        );
    }

    @Override
    public Double getResult(Tuple2<Double, Long> acc) {
        return acc.f1 == 0L ? 0.0 : acc.f0 / acc.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> acc1, Tuple2<Double, Long> acc2) {
        return new Tuple2<>(
                acc1.f0 + acc1.f0,
                acc1.f1 + acc1.f1
        );
    }
}
