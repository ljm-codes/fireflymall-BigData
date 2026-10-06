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

public class topNProcessTool extends ProcessAllWindowFunction<TopNData, DataList<TopNData>, TimeWindow> {

    private Integer topSize = 10;
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    public topNProcessTool() {
    }

    public topNProcessTool(Integer topSize) {
        this.topSize = topSize;
    }

    @Override
    public void process(ProcessAllWindowFunction<TopNData, DataList<TopNData>, TimeWindow>.Context context, Iterable<TopNData> iterable, Collector<DataList<TopNData>> collector) {

        TimeWindow window = context.window();
        String windowStart = formatTimestamp(window.getStart());
        String windowEnd = formatTimestamp(window.getEnd());

        List<TopNData> list = new ArrayList<>();
        System.out.println("iterable: " + iterable);
        for (TopNData tuple4 : iterable) {
            list.add(tuple4);
            System.out.println("全局窗口收集到：" + tuple4);
        }

        // 排序 降序 o2.getCount() - o1.getCount()>0时，为降序
        list.sort((o1, o2) -> o2.getCount() - o1.getCount());

        int actualSize = Math.min(topSize, list.size());

        DataList<TopNData> dataList = new DataList<>();
        List<TopNData> topNDataList = new ArrayList<>();

        // 取前topSize个
        for (int i = 0; i < actualSize; i++) {
            TopNData data = new TopNData();
            TopNData topNData = list.get(i);
            System.out.println(topNData);
            String AD = topNData.getAD();

            data.setGoodsId(topNData.getGoodsId());
            data.setUserId(topNData.getUserId());
            data.setKeyword(topNData.getKeyword());
            data.setMessage(topNData.getMessage());
            data.setSold(topNData.getSold());
            data.setCategory(topNData.getCategory());
            data.setBrand(topNData.getBrand());
            data.setAD(AD);
            data.setCount(topNData.getCount());
            data.setTopN(i + 1);

            topNDataList.add(data);
        }

        dataList.setData(topNDataList);
        dataList.setTotal(actualSize);
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
