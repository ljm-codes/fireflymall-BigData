package com.fm.Tools.FlatMap;

import com.fm.ODS.DWM.SwitchGenuineData;
import com.fm.ODS.DWM.TypeGenuineData;
import com.fm.ODS.RecommendationSystemGenuineData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class SwitchDataFlatMap implements FlatMapFunction<RecommendationSystemGenuineData, SwitchGenuineData> {

    @Override
    public void flatMap(RecommendationSystemGenuineData data, Collector<SwitchGenuineData> collector){
        String userId = data.getUserId();
        Map<String, List<TypeGenuineData>> systemGenuineData = data.getSystemGenuineData();

        for(Map.Entry<String, List<TypeGenuineData>> entry : systemGenuineData.entrySet()){
            SwitchGenuineData switchGenuineData = new SwitchGenuineData();
            String type = entry.getKey();

            switchGenuineData.setUserId(userId);
            switchGenuineData.setType(type);

            List<TypeGenuineData> typeGenuineData = entry.getValue();

            for(TypeGenuineData typeGenuineData1 : typeGenuineData){
                switchGenuineData.setOrderId(typeGenuineData1.getOrderId());
                switchGenuineData.setKeyword(typeGenuineData1.getKeyword());
                switchGenuineData.setMessage(typeGenuineData1.getMessage());
                switchGenuineData.setData(typeGenuineData1.getData());

                collector.collect(switchGenuineData);
            }

        }

    }

}
