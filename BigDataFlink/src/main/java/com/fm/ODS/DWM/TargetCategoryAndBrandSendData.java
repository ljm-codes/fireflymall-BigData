package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TargetCategoryAndBrandSendData {

    private String type;

    private Long goodsId = 0L;
    private String userId = "";
    private String keyword = "";
    private String message = "";
    private Integer sold = 0;
    private String category = "";
    private String brand = "";
    private String AD = "";

    private String windowStart;
    private String windowEnd;

    private Integer count = 1;
    private Integer topN = 0;

    @Override
    public String toString() {
        return "{" +
                "\"type\":\"" + type + "\"" +
                ",\"goodsId\":" + goodsId +
                ",\"userId\":\"" + userId + "\"" +
                ",\"keyword\":\"" + keyword + "\"" +
                ",\"message\":\"" + message + "\"" +
                ",\"sold\":" + sold +
                ",\"category\":\"" + category + "\"" +
                ",\"brand\":\"" + brand + "\"" +
                ",\"AD\":\"" + AD + "\"" +
                ",\"windowStart\":\"" + windowStart + "\"" +
                ",\"windowEnd\":\"" + windowEnd + "\"" +
                ",\"count\":" + count +
                ",\"topN\":" + topN +
                "}";
    }


}
