package com.fm.Tools.FlatMap;

import com.fm.ADS.TopNData;
import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.Main.TopNTypeMainData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class UniversalFlatMap implements FlatMapFunction<TopNTypeMainData, TopNData> {

    @Override
    public void flatMap(TopNTypeMainData topNTypeMainData, Collector<TopNData> collector){
        TopNData middleRankData = new TopNData();
        String userId = topNTypeMainData.getUsername() != null ? topNTypeMainData.getUsername() : topNTypeMainData.getUserIP();
        CommodityRankData commodityRankData = topNTypeMainData.getData().get(0);
        Long goodsId = commodityRankData.getId();
        Integer isAD = commodityRankData.getIsAD();

        middleRankData.setUserId(userId);
        middleRankData.setGoodsId(goodsId);
        middleRankData.setAD(isAD == 1 ? "AD" : "NOT_AD");
        middleRankData.setSold(commodityRankData.getSold());
        middleRankData.setCategory(commodityRankData.getCategory());
        middleRankData.setBrand(commodityRankData.getBrand());
        collector.collect(middleRankData);
    }

}
