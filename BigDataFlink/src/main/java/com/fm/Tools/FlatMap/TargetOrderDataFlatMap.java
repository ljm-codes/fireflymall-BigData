package com.fm.Tools.FlatMap;

import com.fm.ADS.TopNData;
import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.Main.TopNTypeMainData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class TargetOrderDataFlatMap implements FlatMapFunction<TopNTypeMainData, TopNData> {

    @Override
    public void flatMap(TopNTypeMainData topNTypeMainData, Collector<TopNData> collector) throws Exception {
        List<CommodityRankData> commodityRankData = topNTypeMainData.getData();
        String userId = topNTypeMainData.getUsername() != null ? topNTypeMainData.getUsername() : topNTypeMainData.getUserIP();
        for (CommodityRankData commodityData : commodityRankData) {
            TopNData topNData = new TopNData();
            topNData.setUserId(userId);
            topNData.setGoodsId(commodityData.getId());
            topNData.setBrand(commodityData.getBrand());
            topNData.setCategory(commodityData.getCategory());
            topNData.setAD(commodityData.getIsAD() == 1 ? "AD" : "NOT_AD");
            topNData.setSold(commodityData.getSold());
            collector.collect(topNData);
        }

    }
}
