package com.fm.Tools;

import com.fm.ADS.TopNData;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TopNProcess implements WindowFunction<TopNData, TopNData, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow timeWindow, Iterable<TopNData> iterable, Collector<TopNData> collector) throws Exception {
        TopNData data = iterable.iterator().next();
        collector.collect(data);
    }

}
