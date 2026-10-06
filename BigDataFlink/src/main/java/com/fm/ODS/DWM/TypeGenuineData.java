package com.fm.ODS.DWM;

import com.fm.ODS.Main.CommodityRankData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TypeGenuineData {

    private String orderId = "";
    private String keyword = "";
    private String message = "";
    private List<CommodityRankData> data = null;

}
