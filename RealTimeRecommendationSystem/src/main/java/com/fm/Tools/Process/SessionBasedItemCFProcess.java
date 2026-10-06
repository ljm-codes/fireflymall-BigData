package com.fm.Tools.Process;

import com.fm.ADS.RecommendedResults;
import com.fm.ODS.DWM.CommodityRankData;
import com.fm.ODS.DWM.SwitchGenuineData;
import lombok.Getter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.*;
import java.util.stream.Collectors;

@Getter
public class SessionBasedItemCFProcess extends KeyedProcessFunction<String, SwitchGenuineData, RecommendedResults> {

    public enum ProcessType {
        ID,
        SEARCH
    }

    private long sessionTimeout = 30 * 60 * 1000L; // 30分钟
    private int recommendedQuantity = 10; // 推荐商品数量
    private int maxSessionItems = 20; // 每个会话最大商品数量
    private String redisHost = "localhost";
    private int redisPort = 6379;
    private String redisUser = "default";
    private String redisPassword = "123456";
    private ProcessType processType;

    private ListState<Long> sessionItemsState;
    private JedisPool jedisPool;

    private static final Logger LOG = LoggerFactory.getLogger(SessionBasedItemCFProcess.class);

    public SessionBasedItemCFProcess() {
        this(ProcessType.ID);
    }

    public SessionBasedItemCFProcess(ProcessType processType) {
        this.processType = processType;
    }

    public SessionBasedItemCFProcess(long sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public SessionBasedItemCFProcess(int recommendedQuantity) {
        this.recommendedQuantity = recommendedQuantity;
    }

    public SessionBasedItemCFProcess(int recommendedQuantity, int maxSessionItems, String redisHost, int redisPort, String redisPassword, String redisUser) {
        this.recommendedQuantity = recommendedQuantity;
        this.maxSessionItems = maxSessionItems;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisUser = redisUser;
    }

    public SessionBasedItemCFProcess(int recommendedQuantity, int maxSessionItems) {
        this.recommendedQuantity = recommendedQuantity;
        this.maxSessionItems = maxSessionItems;
    }

    public SessionBasedItemCFProcess(int recommendedQuantity, int maxSessionItems, long sessionTimeout, String redisHost, int redisPort, String redisPassword, String redisUser) {
        this.recommendedQuantity = recommendedQuantity;
        this.maxSessionItems = maxSessionItems;
        this.sessionTimeout = sessionTimeout;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.redisUser = redisUser;
    }

    @Override
    public void open(Configuration parameters) {
        sessionItemsState = getRuntimeContext().getListState(new ListStateDescriptor<>("session-items", Long.class));
        jedisPool = new JedisPool(redisHost, redisPort, redisUser, redisPassword);
    }

    @Override
    public void processElement(SwitchGenuineData data, KeyedProcessFunction<String, SwitchGenuineData, RecommendedResults>.Context context, Collector<RecommendedResults> collector) throws Exception {
        List<Long> currentItems = new ArrayList<>();
        sessionItemsState.get().forEach(currentItems::add); //  将会话(已经存在的)商品添加到复制商品列表
        List<CommodityRankData> goodsData = data.getData();

        for (CommodityRankData goodsDatum : goodsData) {
            currentItems.add(goodsDatum.getId());
        }

        if (currentItems.size() > maxSessionItems) {
            currentItems = currentItems.subList(currentItems.size() - maxSessionItems, currentItems.size());
        }

        sessionItemsState.update(currentItems);
        List<Long> recommendations = recommend(currentItems);

        double quantitySaved = this.processType == ProcessType.ID ? 0.7 : 0.3;

        int savedQuantity = (int) (recommendations.size() * quantitySaved);
        if (savedQuantity == 0) {
            savedQuantity = recommendations.size() / 2;
        }

        recommendations = recommendations.subList(0, savedQuantity);

        if (!recommendations.isEmpty()) {
            collector.collect(new RecommendedResults(
                            data.getUserId(),
                            recommendations,
                            "",
                            ""
                    )
            );
        }

        long currentTime = context.timestamp() + sessionTimeout;
        context.timerService().registerEventTimeTimer(currentTime);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, SwitchGenuineData, RecommendedResults>.OnTimerContext ctx, Collector<RecommendedResults> out) throws Exception {
        List<Long> sessionItems = new ArrayList<>();
        sessionItemsState.get().forEach(sessionItems::add);

        Set<Long> uniqueItems = new HashSet<>(sessionItems);

        int weight = this.processType == ProcessType.ID ? 3 : 1;

        if (uniqueItems.size() >= 2) {
            Jedis jedis = jedisPool.getResource();
            try {
                List<Long> itemList = new ArrayList<>(uniqueItems);
                for (int i = 0; i < itemList.size(); i++) {
                    for (int j = i + 1; j < itemList.size(); j++) {
                        Long a = itemList.get(i);
                        Long b = itemList.get(j);
                        // 对称增加共现次数
                        jedis.zincrby("sim:" + a, weight, b.toString());
                        jedis.zincrby("sim:" + b, weight, a.toString());
                    }
                }
            } catch (Exception e) {
                LOG.error("无法将商品共现次数写入Redis：{}", e.getMessage());
            }
        }

        sessionItemsState.clear();
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    private List<Long> recommend(List<Long> sessionItems) {
        if (sessionItems.isEmpty()) return Collections.emptyList();

        Jedis jedis = jedisPool.getResource();

        Map<Long, Double> scoreMap = new HashMap<>();

        try {
            for (Long id : sessionItems) {
                //  从Redis中获取商品id的相似商品列表
                List<Tuple> simItems = jedis.zrevrangeWithScores("sim:" + id, 0, 20);
                for (Tuple t : simItems) {
                    Long candidate = Long.parseLong(t.getElement());
                    if (!sessionItems.contains(candidate)) {
                        double score = t.getScore();
                        scoreMap.merge(candidate, score, Double::sum);
                    }
                }
            }
        } catch (Exception e) {
            return Collections.emptyList();
        }

        return scoreMap.entrySet().stream()
                .sorted(Map.Entry.<Long, Double>comparingByValue().reversed())
                .limit(recommendedQuantity)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

    }

}
