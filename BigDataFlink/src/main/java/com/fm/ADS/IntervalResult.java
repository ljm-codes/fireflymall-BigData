package com.fm.ADS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntervalResult {

    private String windowStart;
    private String windowEnd;
    private String intervalType;
    private Double avgInterval; //  平均间隔（单位：秒）

    @Override
    public String toString() {
        return "{" +
                "\"windowStart\": \"" + windowStart + "\"" +
                ", \"windowEnd\": \"" + windowEnd + "\"" +
                ", \"intervalType\": \"" + intervalType + "\"" +
                ", \"avgInterval\": " + avgInterval +
                '}';
    }

}
