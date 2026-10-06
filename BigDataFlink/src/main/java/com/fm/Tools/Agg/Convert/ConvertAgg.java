package com.fm.Tools.Agg.Convert;

import com.fm.ODS.DWM.BehaviorAnalysisResult;
import org.apache.flink.api.common.functions.AggregateFunction;

public class ConvertAgg implements AggregateFunction<BehaviorAnalysisResult, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(BehaviorAnalysisResult result, Long aLong) {
        return aLong + (result.getValue() == 1.0 ? 1L : 0L);
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
