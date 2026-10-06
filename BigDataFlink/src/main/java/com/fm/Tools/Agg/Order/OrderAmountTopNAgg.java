package com.fm.Tools.Agg.Order;

import com.fm.ODS.DWM.Amount;
import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.Main.TopNTypeMainData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.time.LocalDateTime;
import java.util.List;

public class OrderAmountTopNAgg implements AggregateFunction<TopNTypeMainData, Amount, Amount> {
    @Override
    public Amount createAccumulator() {
        return new Amount();
    }

    @Override
    public Amount add(TopNTypeMainData topNTypeMainData, Amount amount) {
        if (topNTypeMainData == null) {
            return null;
        }
        String userId = topNTypeMainData.getUsername();
        List<CommodityRankData> data = topNTypeMainData.getData();
        double amountValue = 0.0;
        for (CommodityRankData commodityRankData : data) {
            amountValue += (double) (commodityRankData.getQuantity() * commodityRankData.getPrice()) / 100;
        }


        LocalDateTime createTime = topNTypeMainData.getTime();
        return new Amount(userId, amountValue, createTime, -1,"","");
    }

    @Override
    public Amount getResult(Amount amount) {
        return amount;
    }

    @Override
    public Amount merge(Amount amount, Amount acc1) {
        acc1.setAmount(acc1.getAmount() + amount.getAmount());
        return acc1;
    }
}
