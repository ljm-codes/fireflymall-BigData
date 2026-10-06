package com.fm.Tools.Process.WindowAll;

import com.fm.DataList;
import com.fm.ODS.DWM.userIpAndUsername;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class UserIPCountTopN extends ProcessAllWindowFunction<userIpAndUsername, DataList<userIpAndUsername>, TimeWindow> {

    private Integer topN = 10;
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    public UserIPCountTopN() {
    }

    public  UserIPCountTopN(Integer topN) {
        this.topN = topN;
    }
    
    @Override
    public void process(ProcessAllWindowFunction<userIpAndUsername, DataList<userIpAndUsername>, TimeWindow>.Context context, Iterable<userIpAndUsername> iterable, Collector<DataList<userIpAndUsername>> collector) throws Exception {

        TimeWindow window = context.window();
        String windowStart = formatTimestamp(window.getStart());
        String windowEnd = formatTimestamp(window.getEnd());

        // 1. 转换为列表
        List<userIpAndUsername> userIpAndUsernameList = new ArrayList<>();
        for(userIpAndUsername userIpAndUsername : iterable){
            userIpAndUsernameList.add(userIpAndUsername);
        }

        List<userIpAndUsername> filterList = userIpAndUsernameList.stream()
                .sorted((o1, o2) -> Integer.compare(o2.getCount(), o1.getCount()))
                .limit(topN)
                .collect(Collectors.toList());

        // 3. 转换为 DataList 并输出
        DataList<userIpAndUsername> dataList = new DataList<>();

        dataList.setWindowStart(windowStart);
        dataList.setWindowEnd(windowEnd);
        dataList.setData(filterList);
        dataList.setTotal(filterList.size());

        collector.collect(dataList);

    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
