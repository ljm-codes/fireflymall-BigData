package com.fm.Tools.Reduce;

import com.fm.ADS.RecommendedResults;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 合并推荐结果
 */
public class MergeRecommendedResultsReduce implements ReduceFunction<RecommendedResults> {

    @Override
    public RecommendedResults reduce(RecommendedResults data1, RecommendedResults data2) {

        List<Long> merged = new ArrayList<>(data1.getRecommendedGoodsIds());
        for (Long id : data2.getRecommendedGoodsIds()) {
            if (!merged.contains(id)) {
                merged.add(id);
            }
        }
        return new RecommendedResults(
                data1.getUserId(),
                merged,
                data1.getStartTime(),
                data2.getEndTime()
        );
    }

}
