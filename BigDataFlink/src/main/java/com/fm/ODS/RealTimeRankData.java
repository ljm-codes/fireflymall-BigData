package com.fm.ODS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RealTimeRankData {
    private Long goodsId;
    private String userIP;
    private String username;
    private LocalDateTime clickTime;
    private Integer advertisement; // 是否为广告推销（0：不是，1：是）
}
