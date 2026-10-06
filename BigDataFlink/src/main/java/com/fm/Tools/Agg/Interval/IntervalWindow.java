package com.fm.Tools.Agg.Interval;

import com.fm.ADS.IntervalResult;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class IntervalWindow implements WindowFunction<Double, IntervalResult, String, TimeWindow> {

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Double> iterable, Collector<IntervalResult> collector) {

        String windowStart = formatTimestamp(timeWindow.getStart());
        String windowEnd = formatTimestamp(timeWindow.getEnd());

        double avgInterval =iterable.iterator().next();
        collector.collect(new IntervalResult(
                windowStart,
                windowEnd,
                s,
                avgInterval
        ));
    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
