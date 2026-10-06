package com.fm.ADS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SuccessRateWindowResult {

    private String type;
    private String windowStart;
    private String windowEnd;
    private String region;
    private Double successRate;

    @Override
    public String toString() {
        return "{" +
                "\"type\":\"" + type + "\"," +
                "\"windowStart\":\"" + windowStart + "\"," +
                "\"windowEnd\":\"" + windowEnd + "\"," +
                "\"region\":\"" + region + "\"," +
                "\"successRate\":" + successRate +
                "}";
    }

}
