package com.fm.Tools.Process.Diversion;

import com.fm.ADS.RecommendationSystemGenuineData;
import com.fm.ODS.DWM.BehaviorQueue;
import com.fm.ODS.DWM.DetailedBehavior;
import com.fm.ODS.DWM.TypeGenuineData;
import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.Main.TopNTypeMainData;
import lombok.Getter;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;

@Getter
public class RecommendationSystemDataKeyedProcess extends KeyedProcessFunction<String, TopNTypeMainData, RecommendationSystemGenuineData> {

    /**
     * 每个类型最多存储100条数据
     */
    private Integer MAX_BEHAVIORS_PER_TYPE = 100;
    /**
     * 会话超时时间30分钟
     */
    private final long SESSION_TIMEOUT_MS = 1000L;

    private MapState<String, BehaviorQueue> recentBehaviorsState; //  最近行为队列状态，用于存储每个类型的最近行为队列

    private ValueState<Long> sessionTimerState; //  会话定时器状态，用于存储会话过期时间

    private MapState<String, Object> userProfileState; //  用户配置状态，用于存储用户的配置信息


    public RecommendationSystemDataKeyedProcess() {

    }

    public RecommendationSystemDataKeyedProcess(Integer maxBehaviorsPerType) {
        this.MAX_BEHAVIORS_PER_TYPE = maxBehaviorsPerType;
    }

    @Override
    public void open(Configuration parameters) {
        //  创建TTL设置
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(3)) //  30分钟过期
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //  创建时和写入时更新过期时间
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //  从不返回过期状态
                .cleanupFullSnapshot() //  完整快照清理
                .build();

        MapStateDescriptor<String, BehaviorQueue> behaviorsDesc = new MapStateDescriptor<>("behaviors", String.class, BehaviorQueue.class);

        behaviorsDesc.enableTimeToLive(ttlConfig);
        recentBehaviorsState = getRuntimeContext().getMapState(behaviorsDesc);

        ValueStateDescriptor<Long> sessionTimerDesc = new ValueStateDescriptor<>("sessionTimer", Long.class);
        sessionTimerDesc.enableTimeToLive(ttlConfig);
        sessionTimerState = getRuntimeContext().getState(sessionTimerDesc);

        MapStateDescriptor<String, Object> userProfileDesc = new MapStateDescriptor<>("userProfile", String.class, Object.class);
        userProfileDesc.enableTimeToLive(ttlConfig);
        userProfileState = getRuntimeContext().getMapState(userProfileDesc);

    }

    @Override
    public void processElement(TopNTypeMainData topNTypeMainData, KeyedProcessFunction<String, TopNTypeMainData, RecommendationSystemGenuineData>.Context context, Collector<RecommendationSystemGenuineData> collector) throws Exception {
        //  验证
        if (!validateEvent(topNTypeMainData)) {
            return;
        }

        //  转换为详细行为
        DetailedBehavior detailedBehavior = createDetailedBehavior(topNTypeMainData);

        //  更新行为序列
        updateBehaviorSequence(topNTypeMainData.getType(), detailedBehavior);

        //  更新用户画像
        updateUserProfile(detailedBehavior);

        //  注册/更新会话定时器
        registerOrUpdateSessionTimer(context);

    }

    private void registerOrUpdateSessionTimer(Context context) throws IOException {

        if (sessionTimerState.value() == null) {
            long currentTime = context.timerService().currentProcessingTime();
            long nextTrigger = currentTime + SESSION_TIMEOUT_MS;
            context.timerService().registerProcessingTimeTimer(nextTrigger);
            sessionTimerState.update(nextTrigger);
        }

    }

    private void updateUserProfile(DetailedBehavior detailedBehavior) throws Exception {

        String type = detailedBehavior.getType();

        //  1、更新行为计数
        String countKey = type + "_count";
        Integer count = (Integer) userProfileState.get(countKey);
        userProfileState.put(countKey, count != null ? count + 1 : 1);

        //  2、更新最后活跃时间
        userProfileState.put(type + "_last_active", detailedBehavior.getTimestamp());

        //  3、若有商品数据，则更新品类偏好
        if (detailedBehavior.getData() != null) {
            for (CommodityRankData commodityRankData : detailedBehavior.getData()) {
                String category = commodityRankData.getCategory() + "_category";
                Integer categoryCount = (Integer) userProfileState.get(category);
                userProfileState.put(category, categoryCount != null ? categoryCount + 1 : 1);
            }
        }

    }

    private void updateBehaviorSequence(String type, DetailedBehavior detailedBehavior) throws Exception {
        //  获取最近行为队列
        BehaviorQueue behaviorQueue = recentBehaviorsState.get(type);
        if (behaviorQueue == null) {
            behaviorQueue = new BehaviorQueue(MAX_BEHAVIORS_PER_TYPE);
        }

        //  添加最新行为
        behaviorQueue.add(detailedBehavior);
        recentBehaviorsState.put(type, behaviorQueue);

    }

    private DetailedBehavior createDetailedBehavior(TopNTypeMainData topNTypeMainData) {
        DetailedBehavior detailedBehavior = new DetailedBehavior();
        detailedBehavior.setTimestamp(topNTypeMainData.getTime());
        detailedBehavior.setType(topNTypeMainData.getType());
        detailedBehavior.setOrderId(topNTypeMainData.getOrderId());
        detailedBehavior.setKeyword(topNTypeMainData.getKeyword());
        detailedBehavior.setMessage(topNTypeMainData.getMessage());
        detailedBehavior.setData(topNTypeMainData.getData());
        return detailedBehavior;
    }

    private boolean validateEvent(TopNTypeMainData topNTypeMainData) {
        return topNTypeMainData != null
                && topNTypeMainData.getType() != null && !topNTypeMainData.getType().isEmpty()
                && topNTypeMainData.getUserIP() != null && !topNTypeMainData.getUserIP().isEmpty();
    }


    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, TopNTypeMainData, RecommendationSystemGenuineData>.OnTimerContext ctx, Collector<RecommendationSystemGenuineData> out) throws Exception {

        String userId = ctx.getCurrentKey();

        //  1、收集所有行为数据
        Map<String, List<TypeGenuineData>> systemGenuineData = new HashMap<>();

        //  2、遍历

        for (Map.Entry<String, BehaviorQueue> entry : recentBehaviorsState.entries()) {
            String type = entry.getKey();
            BehaviorQueue behaviorQueue = entry.getValue();

            if (behaviorQueue.getSize() > 0) {
                createTypeGenuineDataForOutput(behaviorQueue, systemGenuineData, type);
            }
        }

        //  3、输出
        if (!systemGenuineData.isEmpty()) {
            RecommendationSystemGenuineData output = new RecommendationSystemGenuineData();
            output.setUserId(userId);
            output.setCreateTime(LocalDateTime.now());
            output.setSystemGenuineData(systemGenuineData);

            out.collect(output);
        }

        //  4、清空状态
        recentBehaviorsState.clear();

        long nextTrigger = timestamp + SESSION_TIMEOUT_MS;
        ctx.timerService().registerProcessingTimeTimer(nextTrigger);
        sessionTimerState.update(nextTrigger);

    }

    private void createTypeGenuineDataForOutput(BehaviorQueue behaviorQueue, Map<String, List<TypeGenuineData>> systemGenuineData, String type) {

        for (DetailedBehavior detailedBehavior : behaviorQueue.getAll()) {
            TypeGenuineData data = detailedBehavior.toTypeGenuineData();
            if(data == null){
                continue;
            }
            if (systemGenuineData.containsKey(type)) {
                systemGenuineData.get(type).add(data);
            } else {
                systemGenuineData.put(type, new ArrayList<>(Collections.singletonList(data)));
            }

        }

    }

}
