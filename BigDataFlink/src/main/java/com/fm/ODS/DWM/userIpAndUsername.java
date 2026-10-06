package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class userIpAndUsername {

    private String userIP;
    private List<String> usernameList;
    private Integer count;
    private Integer topN;

}
