package com.fm.ODS.Main;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CommodityRankData {

    private Long id;
    private Integer quantity = 1;
    private Integer price;
    private String image;
    private String category;
    private String brand;
    private Map<String, Object> spec;
    private Integer sold;

    @JsonProperty("isAD")
    private Integer isAD; // 是否为广告商品，1为是，0为否

}
