package com.fm.ODS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SearchRankData<T> {

    private String keyword;
    private String username;
    private String ip;
    private LocalDateTime searchTime;
    private List<T> dataList;

}
