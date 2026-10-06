package com.fm.ADS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehaviorHiveData {

    private String userId;
    private String type;
    private String orderId;
    private String payStatus;
    private String keyword;
    private String message;
    private Long goodsId;
    private Integer quantity;
    private Integer price;
    private String image;
    private String category;
    private String brand;
    private String spec;
    private Integer sold;
    private Integer AD;
    private LocalDateTime createTime;

}
