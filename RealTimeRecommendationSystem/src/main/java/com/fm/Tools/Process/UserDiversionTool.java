package com.fm.Tools.Process;

import com.fm.ODS.RecommendationSystemGenuineData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class UserDiversionTool extends ProcessFunction<RecommendationSystemGenuineData, RecommendationSystemGenuineData> {

    public static final OutputTag<RecommendationSystemGenuineData> USER = new OutputTag<>("CLARIFY_USER", TypeInformation.of(RecommendationSystemGenuineData.class));
    public static final OutputTag<RecommendationSystemGenuineData> SESSION = new OutputTag<>("UNCLEAR_SESSION", TypeInformation.of(RecommendationSystemGenuineData.class));
    public static final OutputTag<RecommendationSystemGenuineData> ERROR = new OutputTag<>("ERROR", TypeInformation.of(RecommendationSystemGenuineData.class));

    @Override
    public void processElement(RecommendationSystemGenuineData value, ProcessFunction<RecommendationSystemGenuineData, RecommendationSystemGenuineData>.Context context, Collector<RecommendationSystemGenuineData> collector) {

        if (value.getUserId() == null || value.getSystemGenuineData() == null) {
            context.output(ERROR, value);
            return;
        }

        String userId = value.getUserId();

        //  判断是明确用户还是匿名用户
        if (isClarifyUser(userId)) {
            //  匿名用户
            context.output(SESSION, value);
        } else {
            context.output(USER, value);
        }

    }

    private boolean isClarifyUser(String userId) {
        String identify = userId.split("_")[0];
        if (identify == null) {
            return false;
        }
        return identify.equals("SESSION");
    }

}
