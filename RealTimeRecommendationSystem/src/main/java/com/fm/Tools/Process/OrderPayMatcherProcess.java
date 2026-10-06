package com.fm.Tools.Process;

import com.fm.ADS.UserBehaviorHiveData;
import lombok.Getter;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.ZoneOffset;

@Getter
public class OrderPayMatcherProcess extends KeyedProcessFunction<String, UserBehaviorHiveData, UserBehaviorHiveData> {

    private ListState<UserBehaviorHiveData> itemState; // 订单状态流中的订单
    private ValueState<String> payState; // 支付状态流中的支付状态
    private ValueState<Long> timerState; // 定时器状态流中的定时器时间

    @Override
    public void open(Configuration parameters) {
        itemState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("items", UserBehaviorHiveData.class));
        payState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pay", String.class));
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer", Long.class));
    }

    @Override
    public void processElement(UserBehaviorHiveData data, KeyedProcessFunction<String, UserBehaviorHiveData, UserBehaviorHiveData>.Context context, Collector<UserBehaviorHiveData> collector) throws Exception {
        String type = data.getType();

        if (type.equals("order")) {
            itemState.add(data);
            String pay = payState.value();
            if (pay != null) {
                data.setPayStatus(pay);
                collector.collect(data);
                return;
            }
            if(timerState.value() == null){
                long timer = data.getCreateTime().toEpochSecond(ZoneOffset.UTC) * 1000L + 60 * 60 * 1000L;
                context.timerService().registerEventTimeTimer(timer);
                timerState.update(timer);
            }

        }else if (type.equals("payStatus")) {
            String newPay = data.getMessage();
            if(newPay.equals("支付成功") || newPay.equals("支付取消")){
                payState.update(newPay);
            }

            boolean hasItems = false;
            String pay = payState.value();
            for (UserBehaviorHiveData item : itemState.get()) {
                item.setPayStatus(pay);
                collector.collect(item);
                hasItems = true;
            }
            if (hasItems) {
                // 已输出所有明细，清空明细状态
                itemState.clear();
                // 支付状态可以保留，但后续不会再有该订单的数据了，可删除定时器
                Long timer = timerState.value();
                if (timer != null) {
                    context.timerService().deleteEventTimeTimer(timer);
                    timerState.clear();
                }
            }

        }

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, UserBehaviorHiveData, UserBehaviorHiveData>.OnTimerContext ctx, Collector<UserBehaviorHiveData> out) throws Exception {
        String pay = payState.value();
        for (UserBehaviorHiveData item : itemState.get()) {
            item.setPayStatus(pay); // pay 可能为 null
            out.collect(item);
        }
        // 清理状态
        itemState.clear();
        payState.clear();
        timerState.clear();
    }
}
