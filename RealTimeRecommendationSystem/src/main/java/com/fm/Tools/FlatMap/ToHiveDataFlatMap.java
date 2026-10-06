package com.fm.Tools.FlatMap;

import com.fm.ADS.UserBehaviorHiveData;
import com.fm.ODS.DWM.CommodityRankData;
import com.fm.ODS.DWM.TypeGenuineData;
import com.fm.ODS.RecommendationSystemGenuineData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class ToHiveDataFlatMap implements FlatMapFunction<RecommendationSystemGenuineData, UserBehaviorHiveData> {

    @Override
    public void flatMap(RecommendationSystemGenuineData data, Collector<UserBehaviorHiveData> collector) {
        String userId = data.getUserId();
        LocalDateTime createTime = data.getCreateTime();
        Map<String, List<TypeGenuineData>> genuineData = data.getSystemGenuineData();
        for (Map.Entry<String, List<TypeGenuineData>> entry : genuineData.entrySet()) {

            String type = entry.getKey();
            List<TypeGenuineData> typeGenuineData = entry.getValue();

            for (TypeGenuineData typeGenuineDataItem : typeGenuineData) {
                String orderId = typeGenuineDataItem.getOrderId();
                String keyword = typeGenuineDataItem.getKeyword();
                String message = typeGenuineDataItem.getMessage();
                List<CommodityRankData> commodityData = typeGenuineDataItem.getData();

                for(CommodityRankData commodityRankData : commodityData) {
                    UserBehaviorHiveData hiveData = new UserBehaviorHiveData(
                            userId,
                            type,
                            orderId,
                            "未支付",
                            keyword,
                            message,
                            commodityRankData.getId(),
                            commodityRankData.getQuantity(),
                            commodityRankData.getPrice(),
                            commodityRankData.getImage(),
                            commodityRankData.getCategory(),
                            commodityRankData.getBrand(),
                            commodityRankData.mapToString(),
                            commodityRankData.getSold(),
                            commodityRankData.getIsAD(),
                            createTime
                    );
                    collector.collect(hiveData);
                }


            }

        }

    }

}
