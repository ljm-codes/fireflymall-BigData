package com.fm.ADS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecommendedResults {

    private String userId;
    private List<Long> recommendedGoodsIds;
    private String startTime;
    private String endTime;

}
