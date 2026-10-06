package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Amount {

    private String userId;
    private Double amount;
    private LocalDateTime createTime;
    private Integer TopN;

    private String windowStart;
    private String windowEnd;

    @Override
    public String toString(){
        return "{"+
                "\"userId\":\""+userId+"\","+
                "\"amount\":"+amount+","+
                "\"createOrderTime\":\""+createTime+"\","+
                "\"TopN\":"+TopN+","+
                "\"windowStart\":\""+windowStart+"\","+
                "\"windowEnd\":\""+windowEnd+"\""+
                "}";
    }

}
