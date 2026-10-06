package com.fm.Tools.Process.WindowAll;

import com.fm.ADS.RecommendCommodityRankData;
import com.fm.ODS.UserSearchHistory;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class RecommendCommodityTopN extends ProcessAllWindowFunction<UserSearchHistory, List<RecommendCommodityRankData>, TimeWindow> {

    private Integer TopN = 10;

    public RecommendCommodityTopN(Integer TopN) {
        this.TopN = TopN;
    }

    public RecommendCommodityTopN() {
    }

    @Override
    public void process(ProcessAllWindowFunction<UserSearchHistory, List<RecommendCommodityRankData>, TimeWindow>.Context context, Iterable<UserSearchHistory> iterable, Collector<List<RecommendCommodityRankData>> collector) throws Exception {
        //  查看数据
        System.out.println("传入的数据为" + iterable);

        //  将数据通过协同推荐算法（item-CF）进行训练转换为一个商品得分矩阵
        //  合并所有数据
        UserSearchHistory userSearchData = new UserSearchHistory();
        for (UserSearchHistory history : iterable) {
            userSearchData.merge(history);
        }
        System.out.println("合并后的用户-商品数据：" + userSearchData);
        Map<String, Double> scoreMatrix = RecommendScoreMatrix(userSearchData);

        //  查看商品得分矩阵
        System.out.println("商品得分矩阵为" + scoreMatrix);

        //  基于得分矩阵进行推荐
        for (String userId : userSearchData.getUserGoodsSet().keySet()) {
            Map<String, List<Long>> recommendCommodityIdByScoreMatrix = RecommendCommodity(userId, scoreMatrix, userSearchData);
            List<RecommendCommodityRankData> recommendCommodityRankDataList = recommendCommodityIdByScoreMatrix
                    .values().stream()
                    .map(longs -> new RecommendCommodityRankData(userId, userSearchData.getUserKeywordSet().get(userId), longs))
                    .collect(Collectors.toList());

            collector.collect(recommendCommodityRankDataList);
        }

    }

    /**
     * 基于商品得分矩阵进行推荐
     *
     * @param s           用户名或IP
     * @param scoreMatrix 商品得分矩阵
     * @param searchCom   商品排名数据列表
     * @return 推荐商品ID列表 userId -> recommendCommodityId
     */
    private Map<String, List<Long>> RecommendCommodity(String s, Map<String, Double> scoreMatrix, UserSearchHistory searchCom) {
        //  寻找用户查询过的商品
        Set<Long> userSearchGoodsIdSet = searchCom.getUserGoodsSet().get(s);
        //  得分列表
        Map<Long, Double> recommendScoreMap = getLongDoubleMap(scoreMatrix, userSearchGoodsIdSet);
        //  排序
        List<Long> recommendedGoods = recommendScoreMap.entrySet().stream()
                .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()))
                .limit(TopN)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        System.out.println("推荐商品ID列表为" + recommendedGoods);

        return Collections.singletonMap(s, recommendedGoods);
    }

    private static Map<Long, Double> getLongDoubleMap(Map<String, Double> scoreMatrix, Set<Long> userSearchGoodsIdSet) {
        Map<Long, Double> recommendScoreMap = new HashMap<>();
        for (Map.Entry<String, Double> entry : scoreMatrix.entrySet()) {
            String commonGoodsId = entry.getKey();
            String[] items = commonGoodsId.split("_");
            Long item1 = Long.parseLong(items[0]);
            Long item2 = Long.parseLong(items[1]);
            double score = entry.getValue();
            //  计算得分
            if (userSearchGoodsIdSet.contains(item1) && !userSearchGoodsIdSet.contains(item2)) {
                recommendScoreMap.put(item2,
                        recommendScoreMap.getOrDefault(item2, 0.0) + score);
            }
        }
        return recommendScoreMap;
    }

    /**
     * item-CF 协同推荐算法
     *
     * @param searchCom 商品排名数据列表
     * @return 商品得分矩阵
     */
    private Map<String, Double> RecommendScoreMatrix(UserSearchHistory searchCom) {
        //  转换成商品-用户的数据集合
        Map<Long, Set<String>> commodityUserSet = searchCom.getGoodsUserSet();
        //  计算商品-商品相似度矩阵
        Map<String, Double> recommendScoreMatrix = new HashMap<>();
        List<Long> allGoodsId = new ArrayList<>(commodityUserSet.keySet());
        for (int i = 0; i < allGoodsId.size(); i++) {
            Long item1 = allGoodsId.get(i);
            Set<String> users1 = commodityUserSet.get(item1);
            for (int j = i + 1; j < allGoodsId.size(); j++) {
                Long item2 = allGoodsId.get(j);
                Set<String> users2 = commodityUserSet.get(item2);

                //  商品共同用户集合
                Set<String> commonUsers = new HashSet<>(users1);
                commonUsers.retainAll(users2);
                int commonUsersSize = commonUsers.size();
                double score = commonUsersSize / Math.sqrt(users1.size() * users2.size());
                String commonKey = item1 + "_" + item2;
                recommendScoreMatrix.put(commonKey, score);
                recommendScoreMatrix.put(item2 + "_" + item1, score);
            }
        }
        return recommendScoreMatrix;
    }


}
