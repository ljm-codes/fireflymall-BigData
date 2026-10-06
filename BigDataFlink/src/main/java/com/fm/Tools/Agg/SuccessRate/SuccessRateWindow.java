package com.fm.Tools.Agg.SuccessRate;

import com.fm.ADS.SuccessRateWindowResult;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class SuccessRateWindow implements WindowFunction<Double, SuccessRateWindowResult, String, TimeWindow> {

    public enum RateType {
        CREATE_ORDER,
        ABANDON_CART,
        CLICK_AD,
        CHANGE_AD
    }

    private final RateType rateType;

    public SuccessRateWindow() {
        this(RateType.CREATE_ORDER);
    }

    public SuccessRateWindow(RateType rateType) {
        this.rateType = rateType;
    }

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<Double> iterable, Collector<SuccessRateWindowResult> collector) {

        String windowStart = formatTimestamp(timeWindow.getStart());
        String windowEnd = formatTimestamp(timeWindow.getEnd());
        String type = rateTypeByRateType(rateType);
        double successRate = iterable.iterator().next();
        collector.collect(new SuccessRateWindowResult(
                type,
                windowStart,
                windowEnd,
                s,
                successRate
        ));
    }

    private String rateTypeByRateType(RateType rateType) {
        switch (rateType) {
            case CREATE_ORDER:
                return "create_order";
            case ABANDON_CART:
                return "abandon_cart";
            case CLICK_AD:
                return "click_ad";
            case CHANGE_AD:
                return "change_ad";
            default:
                return null;
        }
    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }


}
