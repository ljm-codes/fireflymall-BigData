package com.fm.Tools.Agg.SuccessRate;

import com.fm.ODS.Main.TopNTypeMainData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SuccessRateAgg implements AggregateFunction<TopNTypeMainData, Map<String, Long>, Double> {

    public enum SuccessRateType{
        ORDER,
        CART
    }

    private final SuccessRateType successRateType;

    public SuccessRateAgg(){
        this(SuccessRateType.ORDER);
    }

    public SuccessRateAgg(SuccessRateType successRateType){
        this.successRateType = successRateType;
    }

    @Override
    public Map<String, Long> createAccumulator() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, Long> add(TopNTypeMainData topNTypeMainData, Map<String, Long> acc) {
        String message = topNTypeMainData.getMessage();
        //  如果不存在该消息类型，则初始化计数为0
        if (!acc.containsKey(message)) {
            acc.put(message, 0L);
        }
        //  累加该消息类型的计数
        acc.put(message, acc.get(message) + 1L);
        return acc;
    }

    @Override
    public Double getResult(Map<String, Long> acc) {
        String key1 = isSuccessRateType() ? "创建订单" : "add";
        String key2 = isSuccessRateType() ? "取消订单" : "delete";
        long createCount = acc.getOrDefault(key1, 0L);
        long cancelCount = acc.getOrDefault(key2, 0L);
        //  计算创建订单成功率 创建订单数/ （创建订单数 + 取消订单数）
        return createCount == 0 ? 0.0 : (double) createCount / (createCount + cancelCount);
    }

    @Override
    public Map<String, Long> merge(Map<String, Long> acc1, Map<String, Long> acc2) {
        String key1 = isSuccessRateType() ? "创建订单" : "add";
        String key2 = isSuccessRateType() ? "取消订单" : "delete";
        acc1.merge(key1, acc2.getOrDefault(key1, 0L), Long::sum);
        acc1.merge(key2, acc2.getOrDefault(key2, 0L), Long::sum);
        return acc1;
    }

    private boolean isSuccessRateType(){
        return successRateType == SuccessRateType.ORDER;
    }

}
