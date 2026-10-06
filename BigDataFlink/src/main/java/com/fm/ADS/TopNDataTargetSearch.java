package com.fm.ADS;

import com.fm.ODS.Main.CommodityRankData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopNDataTargetSearch {

    private String keyword;
    private List<CommodityRankData> searchList;
    private Integer count;
    private Integer topN;

    private String windowStart;
    private String windowEnd;

}
