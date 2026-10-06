package com.fm.ODS.Main;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopNTypeMainData {

    private String type = "";
    private String username = "";
    private String userIP = "";
    private LocalDateTime time = LocalDateTime.now();
    private String orderId = "";
    private String keyword = "";
    private String message = "";
    private List<CommodityRankData> data = null;

}
