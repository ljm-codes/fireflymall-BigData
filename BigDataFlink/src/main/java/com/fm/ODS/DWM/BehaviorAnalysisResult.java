package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BehaviorAnalysisResult {

    private String analysisType; //    分析类型：FUNNEL（漏斗分析）/INTERVAL（间隔分析）
    private Long windowTime; //    窗口时间
    private String userId; //    用户ID
    private String dimension; //    维度：漏斗步骤（BROWSE/CART）/间隔类型（LAST→CART）
    private Double value; //    值

}
