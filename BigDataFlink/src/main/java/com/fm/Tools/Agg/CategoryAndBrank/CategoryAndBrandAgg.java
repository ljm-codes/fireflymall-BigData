package com.fm.Tools.Agg.CategoryAndBrank;

import com.fm.ADS.TopNData;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class CategoryAndBrandAgg implements AggregateFunction<TopNData, Map<String, Tuple2<List<TopNData>, Integer>>, Map<String, Tuple2<List<TopNData>, Integer>>> {

    public enum AggType {
        CATEGORY,
        BRAND
    }

    public AggType aggType;

    public CategoryAndBrandAgg() {
        this(AggType.CATEGORY);
    }

    public CategoryAndBrandAgg(AggType aggType) {
        this.aggType = Objects.requireNonNull(aggType, "聚合维度不能为空");
    }

    @Override
    public Map<String, Tuple2<List<TopNData>, Integer>> createAccumulator() {
        return new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, Tuple2<List<TopNData>, Integer>> add(TopNData value, Map<String, Tuple2<List<TopNData>, Integer>> acc1) {
        if(value == null){
            return null;
        }
        String aggType = getAggType(value);
        if(StringUtils.isBlank(aggType)){
            return acc1;
        }
        // 累加器中不存在该分类/品牌时，初始化一个空列表和计数1
        acc1.computeIfAbsent(aggType, k -> new Tuple2<>(new CopyOnWriteArrayList<>(), 1));
        Tuple2<List<TopNData>, Integer> tuple2 = acc1.get(aggType);
        List<TopNData> topNDataList = tuple2.f0;
        int dataCount = tuple2.f1;

        int existingIndex = -1;
        for(int i = 0; i < topNDataList.size(); i++){
            if(topNDataList.get(i).equals(value)){
                existingIndex = i;
                break;
            }
        }

        if(existingIndex != -1){
            TopNData topNData = topNDataList.get(existingIndex);
            topNData.setCount(topNData.getCount() + value.getCount());
        } else {
            topNDataList.add(value);
            dataCount++;
        }

        tuple2.f1 = dataCount;

        return acc1;
    }

    @Override
    public Map<String, Tuple2<List<TopNData>, Integer>> getResult(Map<String, Tuple2<List<TopNData>, Integer>> acc1) {
        return acc1;
    }

    @Override
    public Map<String, Tuple2<List<TopNData>, Integer>> merge(Map<String, Tuple2<List<TopNData>, Integer>> acc1, Map<String, Tuple2<List<TopNData>, Integer>> acc2) {
        // 处理acc2的维度，合并到acc1中
        acc2.forEach((dimensionValue, tuple2) -> {
            acc1.computeIfAbsent(dimensionValue, k -> new Tuple2<>(new ArrayList<>(), 0));
            Tuple2<List<TopNData>, Integer> tuple1 = acc1.get(dimensionValue);

            // 合并数据列表
            List<TopNData> mergedList = tuple1.f0;
            for (TopNData data : tuple2.f0) {
                int existingIndex = -1;
                for (int i = 0; i < mergedList.size(); i++) {
                    if (mergedList.get(i).equals(data)) {
                        existingIndex = i;
                        break;
                    }
                }
                if (existingIndex != -1) {
                    mergedList.get(existingIndex).setCount(mergedList.get(existingIndex).getCount() + data.getCount());
                } else {
                    mergedList.add(data);
                }
            }

            // 合并计数（注意：计数需与列表实际元素数一致，此处重新计算更安全）
            tuple1.f1 = mergedList.size();
        });
        return acc1;
    }

    private String getAggType(TopNData topNData) {

        switch (aggType) {
            case CATEGORY:
                return topNData.getCategory();
            case BRAND:
                return topNData.getBrand();
            default:
                return null;
        }

    }
}
