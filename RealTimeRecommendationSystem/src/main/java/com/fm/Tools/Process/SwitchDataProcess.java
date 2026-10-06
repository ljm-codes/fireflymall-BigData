package com.fm.Tools.Process;

import com.fm.ODS.DWM.SwitchGenuineData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 根据type进行数据处理
 * */
public class SwitchDataProcess extends ProcessFunction<SwitchGenuineData, SwitchGenuineData> {

    public static final OutputTag<SwitchGenuineData> ID = new OutputTag<>("goods-ID", TypeInformation.of(SwitchGenuineData.class));
    public static final OutputTag<SwitchGenuineData> SEARCH = new OutputTag<>("goods-SEARCH", TypeInformation.of(SwitchGenuineData.class));
    public static final OutputTag<SwitchGenuineData> CART = new OutputTag<>("goods-CART", TypeInformation.of(SwitchGenuineData.class));
    public static final OutputTag<SwitchGenuineData> ORDER = new OutputTag<>("goods-ORDER", TypeInformation.of(SwitchGenuineData.class));
    public static final OutputTag<SwitchGenuineData> PAY_STATUS = new OutputTag<>("goods-PAY_STATUS", TypeInformation.of(SwitchGenuineData.class));

    @Override
    public void processElement(SwitchGenuineData switchGenuineData, ProcessFunction<SwitchGenuineData, SwitchGenuineData>.Context context, Collector<SwitchGenuineData> collector){
        switch (switchGenuineData.getType()) {
            case "id":
                context.output(ID, switchGenuineData);
                break;
            case "search":
                context.output(SEARCH, switchGenuineData);
                break;
            case "cart":
                context.output(CART, switchGenuineData);
                break;
            case "order":
                context.output(ORDER, switchGenuineData);
                break;
            case "patStatus":
                context.output(PAY_STATUS, switchGenuineData);
                break;
            default:
                collector.collect(switchGenuineData);
                break;
        }
    }

}
