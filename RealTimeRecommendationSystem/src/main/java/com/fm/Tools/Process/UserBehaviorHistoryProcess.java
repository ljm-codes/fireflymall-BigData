package com.fm.Tools.Process;

import com.fm.ADS.RecommendedResults;
import com.fm.ODS.DWM.UserBehaviorHistory;
import lombok.Getter;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class UserBehaviorHistoryProcess extends ProcessAllWindowFunction<UserBehaviorHistory, RecommendedResults, TimeWindow> {

    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zoneId = ZoneId.of("Asia/Shanghai");

    private Integer recommendedQuantity = 10;

    public UserBehaviorHistoryProcess() {

    }

    public UserBehaviorHistoryProcess(Integer recommendedQuantity) {
        this.recommendedQuantity = recommendedQuantity;
    }

    @Override
    public void process(ProcessAllWindowFunction<UserBehaviorHistory, RecommendedResults, TimeWindow>.Context context, Iterable<UserBehaviorHistory> iterable, Collector<RecommendedResults> collector) {
        TimeWindow timeWindow = context.window();
        //  收集全部用户商品行为历史
        UserBehaviorHistory userBehaviorHistory = new UserBehaviorHistory();
        for (UserBehaviorHistory oldData : iterable) {
            userBehaviorHistory.mergeUserBehaviorHistory(oldData);
        }

        System.out.println("UserBehaviorHistory:" +userBehaviorHistory);

        //  计算推荐矩阵
        Map<String, Double> scoreMatrix = RecommendScoreMatrix(userBehaviorHistory);

        System.out.println("scoreMatrix:" + scoreMatrix);

        //  推荐商品
        RecommendedResults recommendGoodsIds = RecommendGoods(scoreMatrix, userBehaviorHistory);

        System.out.println("recommendGoodsIds:" + recommendGoodsIds);

        String windowStart = formatTimestamp(timeWindow.getStart());
        String windowEnd = formatTimestamp(timeWindow.getEnd());

        recommendGoodsIds.setStartTime(windowStart);
        recommendGoodsIds.setEndTime(windowEnd);

        collector.collect(recommendGoodsIds);
    }

    private RecommendedResults RecommendGoods(Map<String, Double> scoreMatrix, UserBehaviorHistory userBehaviorHistory) {
        RecommendedResults results  = new RecommendedResults();

        Map<String, Map<Long, Integer>> userGoodsCountMap = userBehaviorHistory.getUserGoodsCountMap();
        Set<String> userIdSet = userGoodsCountMap.keySet();
        //  计算得分
        for(String userId : userIdSet) {
            List<Long> userBoughtGoodsList = new ArrayList<>(userGoodsCountMap.get(userId).keySet());

            Map<Long, Double> recommendGoodsScoreMap = getRecommendGoodsScoreMap(scoreMatrix, userBoughtGoodsList);

            //  排序
            List<Long> recommendedGoods = recommendGoodsScoreMap.entrySet().stream()
                    .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()))
                    .limit(recommendedQuantity)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            if(recommendedGoods.isEmpty()) {
                //  降级策略
                recommendedGoods = userGoodsCountMap.get(userId).entrySet().stream()
                        .sorted((e1, e2) -> Integer.compare(e2.getValue(), e1.getValue()))
                        .limit(recommendedQuantity)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
            }

            results.setUserId(userId);
            results.setRecommendedGoodsIds(recommendedGoods);

        }
        return results;
    }

    private static Map<Long, Double> getRecommendGoodsScoreMap(Map<String, Double> scoreMatrix, List<Long> userBoughtGoodsList) {
        Map<Long, Double> recommendGoodsScoreMap = new HashMap<>();
        for(Map.Entry<String, Double> entry : scoreMatrix.entrySet()) {
            String commonGoodsId = entry.getKey();
            Double goodsScore = entry.getValue();
            String[] items = commonGoodsId.split("_");
            Long item1 = Long.parseLong(items[0]);
            Long item2 = Long.parseLong(items[1]);

            if(userBoughtGoodsList.contains(item1) && !userBoughtGoodsList.contains(item2)){
                recommendGoodsScoreMap.put(item2,
                        recommendGoodsScoreMap.getOrDefault(item2, 0.0) + goodsScore);
            }

        }
        return recommendGoodsScoreMap;
    }

    private Map<String, Double> RecommendScoreMatrix(UserBehaviorHistory userBehaviorHistory) {
        Map<String, Double> scoreMatrix = new HashMap<>();

        Map<Long, Map<String, Integer>> goodsUserCountMap = userBehaviorHistory.getGoodsUserCountMap();
        Map<String, Integer> userInteractionWeightMap = userBehaviorHistory.getUserInteractionWeightMap();
        Set<Long> goodsIds = goodsUserCountMap.keySet();

        for (Long i : goodsIds) {
            Map<String, Integer> usersOfI = goodsUserCountMap.get(i);
            for (Long j : goodsIds) {
                if (i.equals(j)) {
                    continue;
                }
                Map<String, Integer> usersOfJ = goodsUserCountMap.get(j);
                Set<String> commonUsers = new HashSet<>(usersOfI.keySet());
                commonUsers.retainAll(usersOfJ.keySet());

                double score = 0.0;
                double scoreI2 = 0;
                double scoreJ2 = 0;
                for (String commonUser : commonUsers) {
                    Integer scoreI = usersOfI.get(commonUser);
                    Integer scoreJ = usersOfJ.get(commonUser);
                    if (scoreI <= 0 || scoreJ <= 0) {
                        continue;
                    }
                    int Nu = userInteractionWeightMap.get(commonUser);
                    double iuf = 1.0 / Math.log(1 + Nu);

                    score += scoreI * scoreJ * iuf;
                    scoreI2 += Math.pow(scoreI, 2);
                    scoreJ2 += Math.pow(scoreJ, 2);
                }

                score /= Math.sqrt(scoreI2) * Math.sqrt(scoreJ2);

                scoreMatrix.put(i + "_" + j, score);

            }
        }

        return scoreMatrix;
    }

    private String formatTimestamp(long timestamp) {
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                zoneId
        ).format(formatter);
    }

}
