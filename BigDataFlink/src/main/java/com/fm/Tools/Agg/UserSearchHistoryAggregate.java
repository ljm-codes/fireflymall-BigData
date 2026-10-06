package com.fm.Tools.Agg;

import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.UserSearchHistory;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class UserSearchHistoryAggregate implements AggregateFunction<Tuple3<String, String, CommodityRankData>, UserSearchHistory, UserSearchHistory> {

    @Override
    public UserSearchHistory createAccumulator() {
        return new UserSearchHistory();
    }

    @Override
    public UserSearchHistory add(Tuple3<String, String, CommodityRankData> stringStringCommodityRankDataTuple3, UserSearchHistory userSearchHistory) {
        userSearchHistory.addItem(stringStringCommodityRankDataTuple3.f1, stringStringCommodityRankDataTuple3.f0, stringStringCommodityRankDataTuple3.f2);
        return userSearchHistory;
    }

    @Override
    public UserSearchHistory getResult(UserSearchHistory userSearchHistory) {
        return userSearchHistory;
    }

    @Override
    public UserSearchHistory merge(UserSearchHistory userSearchHistory, UserSearchHistory acc1) {
        userSearchHistory.merge(acc1);
        return userSearchHistory;
    }

}
