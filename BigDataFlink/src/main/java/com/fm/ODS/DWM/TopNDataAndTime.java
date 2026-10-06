package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopNDataAndTime {

    private Long goodsId = 0L;
    private String userId = "";
    private String keyword = "";
    private String message = "";
    private Integer sold = 0;
    private String category = "";
    private String brand = "";
    private String type = "";
    private String AD = "";
    private Integer count = 1;
    private String windowStart;
    private String windowEnd;
    private Integer topN = 0;

    @Override
    public String toString() {
        return "{" +
                "\"goodsId\":" + goodsId +
                ",\"userId\":\"" + userId + "\"" +
                ",\"keyword\":\"" + keyword + "\"" +
                ",\"message\":\"" + message + "\"" +
                ",\"sold\":" + sold +
                ",\"category\":\"" + category + "\"" +
                ",\"brand\":\"" + brand + "\"" +
                ",\"type\":\"" + type + "\"" +
                ",\"AD\":\"" + AD + "\"" +
                ",\"count\":" + count +
                ",\"topN\":" + topN +
                ",\"windowStart\":\"" + windowStart + "\"" +
                ",\"windowEnd\":\"" + windowEnd + "\"" +
                "}";
    }

}
