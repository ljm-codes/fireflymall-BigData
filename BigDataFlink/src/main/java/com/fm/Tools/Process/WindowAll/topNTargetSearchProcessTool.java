package com.fm.Tools.Process.WindowAll;

import com.fm.ADS.TopNDataTargetSearch;
import com.fm.DataList;
import com.fm.ODS.Main.CommodityRankData;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class topNTargetSearchProcessTool extends ProcessAllWindowFunction<Tuple3<String, List<CommodityRankData>, Integer>, DataList<TopNDataTargetSearch>, TimeWindow> {

    private Integer topSize = 10;
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    public topNTargetSearchProcessTool(){

    }

    public  topNTargetSearchProcessTool(Integer topSize){
        this.topSize = topSize;
    }

    /**
     * 针对Search的TopN数据处理
     * @param context 窗口上下文
     * @param iterable 窗口数据迭代器
     * @param collector 数据收集器
     * */
    @Override
    public void process(ProcessAllWindowFunction<Tuple3<String, List<CommodityRankData>, Integer>, DataList<TopNDataTargetSearch>, TimeWindow>.Context context, Iterable<Tuple3<String, List<CommodityRankData>, Integer>> iterable, Collector<DataList<TopNDataTargetSearch>> collector){

        TimeWindow window = context.window();
        String windowStart = formatTimestamp(window.getStart());
        String windowEnd = formatTimestamp(window.getEnd());

        //  合并
        List<Tuple3<String, List<CommodityRankData>, Integer>> list = new ArrayList<>();
        for (Tuple3<String, List<CommodityRankData>, Integer> tuple3 : iterable) {
            list.add(tuple3);
        }
        //  排序
        list.sort((o1, o2) -> o2.f2 - o1.f2);

        List<TopNDataTargetSearch> topNDataList = new ArrayList<>();
        DataList<TopNDataTargetSearch> dataList = new DataList<>();

        int actualSize = Math.min(topSize, list.size());

        for (int i = 0; i < actualSize; i++) {
            Tuple3<String, List<CommodityRankData>, Integer> tuple3 = list.get(i);
            TopNDataTargetSearch topNData = new TopNDataTargetSearch();
            topNData.setTopN(i + 1);
            topNData.setKeyword(tuple3.f0);
            topNData.setCount(tuple3.f2);
            topNData.setSearchList(tuple3.f1);

            topNData.setWindowStart(windowStart);
            topNData.setWindowEnd(windowEnd);

            topNDataList.add(topNData);
        }

        dataList.setData(topNDataList);
        dataList.setTotal(actualSize);

        collector.collect(dataList);

    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
