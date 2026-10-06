package com.fm.ODS.Main;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CommodityRankOutput {
    private String keyword;
    private Long id;
    private Integer quantity = 1;
    private Integer price;
    private String image;
    private String category;
    private String brand;
    private String spec;
    private Integer sold;

    @JsonProperty("isAD")
    private Integer isAD;

    private Integer count;
    private Integer topN;
}