package com.fm.ODS.DWM;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserBehaviorHistory {

    private Map<String, Map<Long, Integer>> userGoodsCountMap = new HashMap<>(); // user -> <goods, score>
    private Map<Long, Map<String, Integer>> goodsUserCountMap = new HashMap<>(); // goods -> <user, score>
    private Map<String, Integer> userInteractionWeightMap = new HashMap<>(); // user -> weight N(u)

    public void addUserBehavior(String userId, Long goodsId, Integer scoreWeight) {

        if (userGoodsCountMap.containsKey(userId)) {
            Map<Long, Integer> goodsScore = userGoodsCountMap.get(userId);
            if (goodsScore.containsKey(goodsId)) {
                Integer score = goodsScore.get(goodsId);
                score += scoreWeight;
                goodsScore.put(goodsId, score);
            } else {
                goodsScore.put(goodsId, scoreWeight);
            }
        } else {
            Map<Long, Integer> goodsScoreMap = new HashMap<>();
            goodsScoreMap.put(goodsId, scoreWeight);
            userGoodsCountMap.put(userId, goodsScoreMap);
        }

        if (goodsUserCountMap.containsKey(goodsId)) {
            Map<String, Integer> goodsScore = goodsUserCountMap.get(goodsId);
            if (goodsScore.containsKey(userId)) {
                Integer score = goodsScore.get(userId);
                score += scoreWeight;
                goodsScore.put(userId, score);
            } else {
                goodsScore.put(userId, scoreWeight);
            }
        } else {
            Map<String, Integer> goodsScore = new HashMap<>();
            goodsScore.put(userId, scoreWeight);
            goodsUserCountMap.put(goodsId, goodsScore);
        }

        if (userInteractionWeightMap.containsKey(userId)) {
            Integer weight = userInteractionWeightMap.get(userId);
            weight += 1;
            userInteractionWeightMap.put(userId, weight);
        }else{
            userInteractionWeightMap.put(userId, 1);
        }

    }

    public void mergeUserBehaviorHistory(UserBehaviorHistory acc) {
        // 合并用户商品行为历史
        for (Map.Entry<String, Map<Long, Integer>> entry : acc.userGoodsCountMap.entrySet()) {
            String userId = entry.getKey();
            Map<Long, Integer> goodsScore = entry.getValue();
            if (this.userGoodsCountMap.containsKey(userId)) {
                Map<Long, Integer> goodsScoreMap = this.userGoodsCountMap.get(userId);
                for (Map.Entry<Long, Integer> entry2 : goodsScore.entrySet()) {
                    Long goodsId = entry2.getKey();
                    Integer score = entry2.getValue();
                    if (goodsScoreMap.containsKey(goodsId)) {
                        Integer score2 = goodsScoreMap.get(goodsId);
                        score2 += score;
                        goodsScoreMap.put(goodsId, score2);
                    } else {
                        goodsScoreMap.put(goodsId, score);
                    }
                }
            } else {
                this.userGoodsCountMap.put(userId, goodsScore);
            }
        }

        //  同理合并商品用户行为历史
        for (Map.Entry<Long, Map<String, Integer>> entry : acc.goodsUserCountMap.entrySet()) {
            Long goodsId = entry.getKey();
            Map<String, Integer> goodsScore = entry.getValue();
            if (this.goodsUserCountMap.containsKey(goodsId)) {
                Map<String, Integer> goodsScoreMap = this.goodsUserCountMap.get(goodsId);
                for (Map.Entry<String, Integer> entry2 : goodsScore.entrySet()) {
                    String userId = entry2.getKey();
                    Integer score = entry2.getValue();
                    if (goodsScoreMap.containsKey(userId)) {
                        Integer score2 = goodsScoreMap.get(userId);
                        score2 += score;
                        goodsScoreMap.put(userId, score2);
                    } else {
                        goodsScoreMap.put(userId, score);
                    }
                }
            } else {
                this.goodsUserCountMap.put(goodsId, goodsScore);
            }
        }

        for(Map.Entry<String, Integer> entry : acc.userInteractionWeightMap.entrySet()){
            String userId = entry.getKey();
            Integer score = entry.getValue();
            if (this.userInteractionWeightMap.containsKey(userId)) {
                Integer score2 = this.userInteractionWeightMap.get(userId);
                score2 += score;
                this.userInteractionWeightMap.put(userId, score2);
            } else {
                this.userInteractionWeightMap.put(userId, score);
            }
        }

    }

}
