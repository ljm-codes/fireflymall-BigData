package com.fm.ADS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopNData {

    private Long goodsId = 0L;
    private String userId = "";
    private String keyword = "";
    private String message = "";
    private Integer sold = 0;
    private String category = "";
    private String brand = "";
    private String AD = "";
    private Integer count = 1;
    private Integer topN = 0;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopNData topNData = (TopNData) o;
        return Objects.equals(category, topNData.category) &&
                Objects.equals(brand, topNData.brand); // 按需调整相等性判断规则
    }

    @Override
    public int hashCode() {
        return Objects.hash(category, brand);
    }

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
                ",\"AD\":\"" + AD + "\"" +
                ",\"count\":" + count +
                ",\"topN\":" + topN +
                "}";
    }
}