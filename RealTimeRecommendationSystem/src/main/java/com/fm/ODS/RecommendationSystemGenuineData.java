package com.fm.ODS;

import com.fm.ODS.DWM.TypeGenuineData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RecommendationSystemGenuineData {

    private String userId; //   username or userIp / if username is null, userId=userIp
    private LocalDateTime createTime;
    private Map<String, List<TypeGenuineData>> systemGenuineData;

}
