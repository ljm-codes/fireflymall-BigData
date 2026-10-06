package com.fm.ODS;

import com.fm.ODS.Main.CommodityRankData;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserSearchHistory {

    private Map<String, Set<Long>> userGoodsSet = new HashMap<>();
    private Map<Long, Set<String>> goodsUserSet = new HashMap<>();
    private Map<String, String> userKeywordSet = new HashMap<>();

    public void addItem(String userId, String keyword, CommodityRankData goods) {

        if (userId == null || goods == null) {
            return;
        }
        Set<Long> items = userGoodsSet.computeIfAbsent(userId, k -> new HashSet<>());
        boolean added = items.add(goods.getId());

        userKeywordSet.put(userId, keyword);

        if (added) {
            Set<String> users = goodsUserSet.computeIfAbsent(goods.getId(), k -> new HashSet<>());
            users.add(userId);

        }

    }

    public void merge(UserSearchHistory other) {
        if (other == null) {
            return;
        }
        for (Map.Entry<String, Set<Long>> entry : other.userGoodsSet.entrySet()) {
            String userId = entry.getKey();
            Set<Long> otherGoodsIds = entry.getValue();

            String otherKeyword = other.userKeywordSet.get(userId);
            userKeywordSet.put(userId, otherKeyword);

            Set<Long> currentItems = userGoodsSet.computeIfAbsent(userId, k -> new HashSet<>());
            currentItems.addAll(otherGoodsIds);

            for (Long otherGoodsId : otherGoodsIds) {
                goodsUserSet.computeIfAbsent(otherGoodsId, k -> new HashSet<>()).add(userId);
            }

        }
    }

}
