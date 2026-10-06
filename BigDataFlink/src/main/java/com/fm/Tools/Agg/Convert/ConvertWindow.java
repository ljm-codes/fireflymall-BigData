package com.fm.Tools.Agg.Convert;

import com.fm.ADS.FunnelResult;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ConvertWindow implements WindowFunction<Long, FunnelResult, String, TimeWindow> {

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<FunnelResult> collector) {

        String windowStart = formatTimestamp(timeWindow.getStart());
        String windowEnd = formatTimestamp(timeWindow.getEnd());

        long count = iterable.iterator().next();
        collector.collect(new FunnelResult(
                windowStart,
                windowEnd,
                s,
                count
        ));
    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
