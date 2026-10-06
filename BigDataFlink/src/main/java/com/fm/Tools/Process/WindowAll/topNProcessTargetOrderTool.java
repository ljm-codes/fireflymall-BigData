package com.fm.Tools.Process.WindowAll;

import com.fm.ADS.TopNData;
import com.fm.DataList;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class topNProcessTargetOrderTool extends ProcessAllWindowFunction<TopNData, DataList<TopNData>, TimeWindow> {

    private Integer topSize = 10;
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    public topNProcessTargetOrderTool() {
    }

    public topNProcessTargetOrderTool(Integer topSize) {
        this.topSize = topSize;
    }

    @Override
    public void process(ProcessAllWindowFunction<TopNData, DataList<TopNData>, TimeWindow>.Context context, Iterable<TopNData> iterable, Collector<DataList<TopNData>> collector) {

        TimeWindow window = context.window();
        String windowStart = formatTimestamp(window.getStart());
        String windowEnd = formatTimestamp(window.getEnd());

        List<TopNData> list = new ArrayList<>();

        for (TopNData topNData : iterable) {
            list.add(topNData);
            System.out.println("全局窗口收集到：" + topNData);
        }

        list.sort((d1, d2) -> d2.getSold() - d1.getSold());

        int size = Math.min(topSize, list.size());

        DataList<TopNData> dataList = new DataList<>();
        List<TopNData> topNDataBySoldList = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            TopNData topNData = list.get(i);
            topNData.setTopN(i + 1);
            topNData.setCount(-1);
            topNDataBySoldList.add(topNData);
        }

        dataList.setData(topNDataBySoldList);
        dataList.setTotal(size);
        dataList.setWindowStart(windowStart);
        dataList.setWindowEnd(windowEnd);

        collector.collect(dataList);

    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
