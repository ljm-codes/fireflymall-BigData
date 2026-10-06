package com.fm.ADS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecommendCommodityRankData {

    private String usernameOrIp;
    private String keyWord;
    private List<Long> recommendCommodityId;

}
