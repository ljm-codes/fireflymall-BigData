package com.fm.ODS.DWM;

import com.fm.ODS.Main.CommodityRankData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DetailedBehavior {

    private LocalDateTime timestamp;
    private String type;
    private String orderId;
    private String keyword;
    private String message;
    private List<CommodityRankData> data;

    public TypeGenuineData toTypeGenuineData() {

        TypeGenuineData data = new TypeGenuineData();
        data.setOrderId(this.orderId);
        data.setKeyword(this.keyword);
        data.setMessage(this.message);
        data.setData(this.data);
        return data;

    }

}
