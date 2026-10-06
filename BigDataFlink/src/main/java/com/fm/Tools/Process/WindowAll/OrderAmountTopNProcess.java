package com.fm.Tools.Process.WindowAll;

import com.fm.ODS.DWM.Amount;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class OrderAmountTopNProcess extends ProcessAllWindowFunction<Amount, List<Amount>, TimeWindow> {

    private Integer topN = 10;
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    public OrderAmountTopNProcess() {
    }

    public OrderAmountTopNProcess(Integer topN) {
        this.topN = topN;
    }

    @Override
    public void process(ProcessAllWindowFunction<Amount, List<Amount>, TimeWindow>.Context context, Iterable<Amount> iterable, Collector<List<Amount>> collector) throws Exception {

        TimeWindow window = context.window();
        String windowStart = formatTimestamp(window.getStart());
        String windowEnd = formatTimestamp(window.getEnd());

        List<Amount> amount = new ArrayList<>();
        for (Amount a : iterable) {
            a.setWindowStart(windowStart);
            a.setWindowEnd(windowEnd);
            amount.add(a);
        }

        amount = amount.stream()
                .sorted(Comparator.comparingDouble(Amount::getAmount))
                .limit(topN)
                .collect(Collectors.toList());

        for (int i = 0; i < amount.size(); i++) {
            amount.get(i).setTopN(i + 1);
        }

        collector.collect(amount);

    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
