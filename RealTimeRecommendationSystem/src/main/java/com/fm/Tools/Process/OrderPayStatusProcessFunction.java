package com.fm.Tools.Process;

import com.fm.ADS.UserBehaviorHiveData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderPayStatusProcessFunction extends ProcessFunction<UserBehaviorHiveData, UserBehaviorHiveData> {

    public static final OutputTag<UserBehaviorHiveData> ORDER_PAY = new OutputTag<>("ORDER_PAY", TypeInformation.of(UserBehaviorHiveData.class));

    @Override
    public void processElement(UserBehaviorHiveData data, ProcessFunction<UserBehaviorHiveData, UserBehaviorHiveData>.Context context, Collector<UserBehaviorHiveData> collector){
        String type = data.getType();
        switch (type) {
            case "order":
            case "payStatus":
                context.output(ORDER_PAY, data);
                break;
            default:
                collector.collect(data);
                break;
        }
    }


}
