package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserIpRegionData {

    private String userIP;
    private String username;
    private String windowStart;
    private String windowEnd;
    private Integer count;
    private Integer topN;

    @Override
    // Json
    public String toString() {
        return "{" +
                "\"userIP\":" + userIP +
                ",\"username\":\"" + username + "\"" +
                ",\"count\":" + count +
                ",\"topN\":" + topN +
                ",\"windowStart\":\"" + windowStart + "\"" +
                ",\"windowEnd\":\"" + windowEnd + "\"" +
                "}";

    }

}
