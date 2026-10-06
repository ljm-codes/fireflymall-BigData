package com.fm.Tools.Agg;

import com.fm.ODS.DWM.CommodityRankData;
import com.fm.ODS.DWM.SwitchGenuineData;
import com.fm.ODS.DWM.UserBehaviorHistory;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.List;

public class UserBehaviorHistoryAgg implements AggregateFunction<SwitchGenuineData, UserBehaviorHistory, UserBehaviorHistory> {

    @Override
    public UserBehaviorHistory createAccumulator() {
        return new UserBehaviorHistory();
    }

    @Override
    public UserBehaviorHistory add(SwitchGenuineData value, UserBehaviorHistory acc) {
        int scoreWeight = 1;
        String userId = value.getUserId();
        String type = value.getType();
        String message = value.getMessage();
        switch (type) {
            case "id":
                scoreWeight = 3;
                break;
            case "search":
                break;
            case "cart":
                if(message.equals("add")){
                    scoreWeight = 5;
                }else if(message.equals("delete")){
                    scoreWeight = -8;
                }
                break;
            case "order":
                if(message.equals("创建订单")){
                    scoreWeight = 10;
                }else if(message.equals("取消订单")){
                    scoreWeight = -15;
                }
                break;

        }
        List<CommodityRankData> data = value.getData();
        for(CommodityRankData rankData : data){
            Long goodsId = rankData.getId();
            acc.addUserBehavior(userId, goodsId, scoreWeight);
        }
        return acc;
    }

    @Override
    public UserBehaviorHistory getResult(UserBehaviorHistory acc) {
        return acc;
    }

    @Override
    public UserBehaviorHistory merge(UserBehaviorHistory acc1, UserBehaviorHistory acc2) {
        acc1.mergeUserBehaviorHistory(acc2);
        return acc1;
    }

}
