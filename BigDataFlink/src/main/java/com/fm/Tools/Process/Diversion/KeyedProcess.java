package com.fm.Tools.Process.Diversion;

import com.fm.ODS.DWM.BehaviorAnalysisResult;
import com.fm.ODS.DWM.UserBehavior;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * <h1>核心处理类</h1>
 * <p>追踪行为轨迹+计算间隔时间</p>
 * <p>{@link KeyedProcessFunction}: </p>
 * <p>String: 用户名</p>
 * <p>UserBehavior: 用户行为</p>
 * <p>BehaviorAnalysisResult: 行为分析结果</p>
 */
public class KeyedProcess extends KeyedProcessFunction<String, UserBehavior, BehaviorAnalysisResult> {

    private ListState<UserBehavior> behaviorListState; //   保存用户行为轨迹

    private ValueState<Long> lastBehaviorTimestampState; //   保存上一个行为的时间戳(计算间隔时间)

    private MapState<String, Boolean> funnelStepState; //   保存漏斗步骤状态(避免重复计算)

    /**
     * 初始化处理
     */
    @Override
    public void open(Configuration parameters) {
        //  创建TTL配置，设置10分钟过期(测试)
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        //  初始化 behaviorListState
        ListStateDescriptor<UserBehavior> trackDesc = new ListStateDescriptor<>(
                "userBehaviorTrack",
                UserBehavior.class
        );
        trackDesc.enableTimeToLive(ttlConfig);
        behaviorListState = getRuntimeContext().getListState(trackDesc);

        //  同理为 lastBehaviorTimestampState 和 funnelStepState 进行初始化
        ValueStateDescriptor<Long> tsDesc = new ValueStateDescriptor<>(
                "lastBehaviorTs",
                Long.class
        );
        tsDesc.enableTimeToLive(ttlConfig);
        lastBehaviorTimestampState = getRuntimeContext().getState(tsDesc);

        MapStateDescriptor<String, Boolean> funnelDesc = new MapStateDescriptor<>(
                "funnelStepFlag",
                String.class,
                Boolean.class
        );
        funnelDesc.enableTimeToLive(ttlConfig);
        funnelStepState = getRuntimeContext().getMapState(funnelDesc);

    }
    /**
     * <h2>处理元素</h2>
     * <p>根据用户行为类型进行处理</p>
     * <p>1. 保存用户行为轨迹</p>
     * <p>2. 计算行为间隔并输出</p>
     * <p>3. 更新上一个行为的时间戳</p>
     * <p>4. 注册定时器</p>
     */
    @Override
    public void processElement(UserBehavior userBehavior, KeyedProcessFunction<String, UserBehavior, BehaviorAnalysisResult>.Context context, Collector<BehaviorAnalysisResult> collector) throws Exception {
        //  保存用户行为轨迹
        behaviorListState.add(userBehavior);

        //  计算行为间隔并输出
        Long lastTs = lastBehaviorTimestampState.value();
        if (lastTs != null && userBehavior.getTimestamp() > lastTs) {
            long interval = userBehavior.getTimestamp() - lastTs;
            //  输出间隔时间
            collector.collect(new BehaviorAnalysisResult(
                    "INTERVAL",
                    context.timerService().currentWatermark(),
                    userBehavior.getUsername(),
                    "LAST → " + userBehavior.getBehaviorType(),
                    (double) interval / 1000 //  转换为秒
            ));
        }

        //  更新上一个行为的时间戳
        lastBehaviorTimestampState.update(userBehavior.getTimestamp());

        //  注册定时器, 到时间执行onTimer方法
        long windowEnd = (userBehavior.getTimestamp() / (5 * 60 * 1000)) * 5 * 60 * 1000 + 5 * 60 * 1000;
        context.timerService().registerEventTimeTimer(windowEnd);

    }
    /**
     * <h2>处理定时器</h2>
     * <p>1. 输出漏斗分析结果</p>
     * <p>2. 清空状态</p>
     */
    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, UserBehavior, BehaviorAnalysisResult>.OnTimerContext ctx, Collector<BehaviorAnalysisResult> out) throws Exception {

        List<UserBehavior> behaviorList = new ArrayList<>();
        for (UserBehavior userBehavior : behaviorListState.get()) {
            behaviorList.add(userBehavior);
        }

        if (behaviorList.isEmpty()) {
            return;
        }

        //  按时间戳排序
        behaviorList.sort(Comparator.comparingLong(UserBehavior::getTimestamp));

        boolean hasBrowse = false;
        boolean hasCart = false;
        boolean hasOrder = false;
        boolean hasPay = false;

        for (UserBehavior userBehavior : behaviorList) {
            switch (userBehavior.getBehaviorType()) {

                case "search":
                case "id":
                    hasBrowse = true;
                    break;
                case "cart":
                    if (hasBrowse && !funnelStepState.contains("cart")) {
                        funnelStepState.put("cart", true);
                        hasCart = true;
                    }
                    break;
                case "order":
                    if (hasCart && !funnelStepState.contains("order")) {
                        funnelStepState.put("order", true);
                        hasOrder = true;
                    }
                    break;
                case "pay":
                    if (hasOrder && !funnelStepState.contains("pay")) {
                        funnelStepState.put("pay", true);
                        hasPay = true;
                    }
                    break;
            }
        }

        //  输出漏斗结果
        out.collect(new BehaviorAnalysisResult("FUNNEL", timestamp, ctx.getCurrentKey(), "BROWSE", hasBrowse ? 1.0 : 0.0));
        out.collect(new BehaviorAnalysisResult("FUNNEL", timestamp, ctx.getCurrentKey(), "CART", hasCart ? 1.0 : 0.0));
        out.collect(new BehaviorAnalysisResult("FUNNEL", timestamp, ctx.getCurrentKey(), "ORDER", hasOrder ? 1.0 : 0.0));
        out.collect(new BehaviorAnalysisResult("FUNNEL", timestamp, ctx.getCurrentKey(), "PAY", hasPay ? 1.0 : 0.0));

        //  清空状态
        behaviorListState.clear();
        lastBehaviorTimestampState.clear();
        funnelStepState.clear();

    }

}
