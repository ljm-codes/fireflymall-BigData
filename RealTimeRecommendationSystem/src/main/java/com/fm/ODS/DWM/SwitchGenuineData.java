package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SwitchGenuineData {

    private String userId;
    private String type;
    private String orderId = "";
    private String keyword = "";
    private String message = "";
    private List<CommodityRankData> data = null;

}
