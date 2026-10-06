package com.fm.Tools.Process.WindowAll;


import com.fm.ADS.CategoryOrBrandTopNData;
import com.fm.ADS.TopNData;
import com.fm.DataList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TargetCategoryAndBrandTopN extends ProcessAllWindowFunction<Map<String, Tuple2<List<TopNData>, Integer>>, DataList<CategoryOrBrandTopNData>, TimeWindow> {

    private int topN = 10;
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    public TargetCategoryAndBrandTopN() {
    }

    public TargetCategoryAndBrandTopN(int topN) {
        this.topN = topN;
        if (topN <= 0) {
            throw new IllegalArgumentException("TopN 数量必须大于0");
        }
    }

    @Override
    public void process(ProcessAllWindowFunction<Map<String, Tuple2<List<TopNData>, Integer>>, DataList<CategoryOrBrandTopNData>, TimeWindow>.Context context, Iterable<Map<String, Tuple2<List<TopNData>, Integer>>> iterable, Collector<DataList<CategoryOrBrandTopNData>> collector) {

        TimeWindow window = context.window();
        String windowStart = formatTimestamp(window.getStart());
        String windowEnd = formatTimestamp(window.getEnd());

        //  合并
        Map<String, Tuple2<List<TopNData>, Integer>> map = getMergedMap(iterable);

        if (map.isEmpty()) {
            collector.collect(new DataList<>());
            return;
        }

        //  排序：先排序category或者brand，再进行topN，再排序内部商品列表
        List<Map.Entry<String, Tuple2<List<TopNData>, Integer>>> sortedCategoryBrand = map.entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e2.getValue().f1, e1.getValue().f1))
                .limit(topN)
                .collect(Collectors.toList());

        List<CategoryOrBrandTopNData> categoryOrBrandTopNDataList = new ArrayList<>();
        DataList<CategoryOrBrandTopNData> dataList = new DataList<>();
        for (int i = 0; i < sortedCategoryBrand.size(); i++) {
            Map.Entry<String, Tuple2<List<TopNData>, Integer>> entry = sortedCategoryBrand.get(i);
            String key = entry.getKey();
            Tuple2<List<TopNData>, Integer> value = entry.getValue();

            List<TopNData> productList = value.f0;
            int size = productList.size();

            if(size >=5000){
                productList = productList.parallelStream()
                        .sorted((e1, e2) -> Integer.compare(e2.getSold(), e1.getSold()))
                        .collect(Collectors.toList());
            }else{
                productList.sort((p1,p2) -> Integer.compare(p2.getSold(), p1.getSold()));
            }

            for (int j = 0; j < productList.size(); j++) {
                productList.get(j).setTopN(j + 1);
            }

            CategoryOrBrandTopNData topNData = new CategoryOrBrandTopNData();

            topNData.setTopNData(productList);
            topNData.setCategoryOrBrand(key);
            topNData.setCount(value.f1);
            topNData.setTopN(i + 1);

            categoryOrBrandTopNDataList.add(topNData);

        }

        dataList.setData(categoryOrBrandTopNDataList);
        dataList.setTotal(sortedCategoryBrand.size());
        dataList.setWindowStart(windowStart);
        dataList.setWindowEnd(windowEnd);

        collector.collect(dataList);
    }

    private static Map<String, Tuple2<List<TopNData>, Integer>> getMergedMap(Iterable<Map<String, Tuple2<List<TopNData>, Integer>>> iterable) {
        Map<String, Tuple2<List<TopNData>, Integer>> map = new HashMap<>();
        for (Map<String, Tuple2<List<TopNData>, Integer>> item : iterable) {
            for (Map.Entry<String, Tuple2<List<TopNData>, Integer>> entry : item.entrySet()) {
                String key = entry.getKey();
                Tuple2<List<TopNData>, Integer> value = entry.getValue();

                map.merge(key, value, (oldValue, newValue) -> {
                    // 1. 合并商品TopN数据列表（去重+保留全量）
                    List<TopNData> mergedDataList = new ArrayList<>(oldValue.f0);
                    mergedDataList.addAll(newValue.f0);
                    // 2. 累加总计数
                    Integer mergedCount = oldValue.f1 + newValue.f1;
                    return new Tuple2<>(mergedDataList, mergedCount);
                });
            }
        }
        return map;
    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
