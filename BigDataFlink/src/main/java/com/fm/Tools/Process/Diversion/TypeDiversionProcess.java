package com.fm.Tools.Process.Diversion;

import com.fm.ODS.Main.TopNTypeMainData;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class TypeDiversionProcess extends ProcessFunction<TopNTypeMainData, TopNTypeMainData> {

    public static final OutputTag<TopNTypeMainData> ID_DATA = new OutputTag<>("ID", TypeInformation.of(TopNTypeMainData.class)); //    点击商品侧输出流
    public static final OutputTag<TopNTypeMainData> SEARCH_DATA = new OutputTag<>("SEARCH", TypeInformation.of(TopNTypeMainData.class)); //    搜索商品侧输出流
    public static final OutputTag<TopNTypeMainData> ORDER_DATA = new OutputTag<>("ORDER", TypeInformation.of(TopNTypeMainData.class)); //  订单商品侧输出流
    public static final OutputTag<TopNTypeMainData> CART_DATA = new OutputTag<>("CART", TypeInformation.of(TopNTypeMainData.class)); //    购物车商品侧输出流
    public static final OutputTag<TopNTypeMainData> PAY_DATA = new OutputTag<>("PAY", TypeInformation.of(TopNTypeMainData.class)); //  支付订单侧输出流

    @Override
    public void processElement(TopNTypeMainData topNTypeMainData, ProcessFunction<TopNTypeMainData, TopNTypeMainData>.Context context, Collector<TopNTypeMainData> collector) throws Exception {
        String type = topNTypeMainData.getType();
        switch (type) {
            case "id":
                context.output(ID_DATA, topNTypeMainData);
                break;
            case "search":
                context.output(SEARCH_DATA, topNTypeMainData);
                break;
            case "order":
                context.output(ORDER_DATA, topNTypeMainData);
                break;
            case "cart":
                context.output(CART_DATA, topNTypeMainData);
                break;
            case "payStatus":
                context.output(PAY_DATA, topNTypeMainData);
                break;
            default:
                System.err.println("没找到对应流，进行主流处理");
                collector.collect(topNTypeMainData);
                break;
        }

    }
}
