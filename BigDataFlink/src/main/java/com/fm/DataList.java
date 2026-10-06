package com.fm;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataList<T> {

    private List<T> data; // 数据列表
    private Integer total; // 总条数
    private float totalPrice = 0.0f; // 总金额
    private Integer totalQuantity = 0; // 总数量

    private String windowStart;
    private String windowEnd;

}
