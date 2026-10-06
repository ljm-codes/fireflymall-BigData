package com.fm.Tools.FlatMap;

import com.fm.ADS.RecommendedResults;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class ToJsonDataFlatMap implements FlatMapFunction<RecommendedResults, String> {

    @Override
    public void flatMap(RecommendedResults data, Collector<String> collector) {
        List<Long> recommendGoodsIds = data.getRecommendedGoodsIds();
        for (Long goodsId : recommendGoodsIds) {
            String json = "{" +
                    "\"userId\":\"" + data.getUserId() + "\"," +
                    "\"goodsId\":\"" + goodsId + "\"," +
                    "\"startTime\":\"" + data.getStartTime() + "\"," +
                    "\"endTime\":\"" + data.getEndTime() + "\"" +
                    "}";
            collector.collect(json);
        }
    }

}
