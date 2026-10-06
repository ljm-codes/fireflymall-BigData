package com.fm.Tools.FlatMap;

import com.fm.ADS.RecommendedResults;
import lombok.Getter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

@Getter
public class SplitRecommendedGoodsListByWeight implements FlatMapFunction<RecommendedResults, RecommendedResults> {

    private double weight = 0.1;

    public SplitRecommendedGoodsListByWeight() {
    }

    public SplitRecommendedGoodsListByWeight(double weight) {
        if(weight <= 0 || weight >= 1){
            throw new IllegalArgumentException("weight must be greater than 0 and less than 1");
        }
        this.weight = weight;
    }

    @Override
    public void flatMap(RecommendedResults recommendedResults, Collector<RecommendedResults> collector){
        List<Long> recommendGoodsList = recommendedResults.getRecommendedGoodsIds();
        if(recommendGoodsList.isEmpty()){
            collector.collect(recommendedResults);
            return;
        }

        int splitIndex = (int) (recommendGoodsList.size() * weight);
        if(splitIndex == 0){
            //  保护机制
            splitIndex = recommendGoodsList.size() / 2;
        }
        List<Long> firstPart = recommendGoodsList.subList(0, splitIndex);
        recommendedResults.setRecommendedGoodsIds(firstPart);

        collector.collect(recommendedResults);

    }


}
