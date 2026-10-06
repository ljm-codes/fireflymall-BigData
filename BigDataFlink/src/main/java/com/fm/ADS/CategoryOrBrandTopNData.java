package com.fm.ADS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CategoryOrBrandTopNData {

    private String categoryOrBrand;
    private List<TopNData> topNData;
    private Integer count;
    private Integer topN;

}
