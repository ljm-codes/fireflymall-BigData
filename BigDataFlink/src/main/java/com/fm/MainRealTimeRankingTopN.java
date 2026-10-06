package com.fm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fm.ADS.*;
import com.fm.ODS.DWM.*;
import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.Main.TopNTypeMainData;
import com.fm.Tools.Agg.CategoryAndBrank.CategoryAndBrandAgg;
import com.fm.Tools.Agg.Click.ClickADRateAgg;
import com.fm.Tools.Agg.Convert.ConvertAgg;
import com.fm.Tools.Agg.Convert.ConvertWindow;
import com.fm.Tools.Agg.CountAgg;
import com.fm.Tools.Agg.Interval.IntervalAgg;
import com.fm.Tools.Agg.Interval.IntervalWindow;
import com.fm.Tools.Agg.Order.CountTargetOrderAgg;
import com.fm.Tools.Agg.Order.OrderAmountTopNAgg;
import com.fm.Tools.Agg.Pay.PayStatusAgg;
import com.fm.Tools.Agg.Search.CountTargetSearchAgg;
import com.fm.Tools.Agg.SuccessRate.SuccessRateAgg;
import com.fm.Tools.Agg.SuccessRate.SuccessRateWindow;
import com.fm.Tools.Agg.UserBehavior.UserIPCountAgg;
import com.fm.Tools.FlatMap.TargetOrderDataFlatMap;
import com.fm.Tools.FlatMap.UniversalFlatMap;
import com.fm.Tools.Process.Diversion.KeyedProcess;
import com.fm.Tools.Process.Diversion.RecommendationSystemDataKeyedProcess;
import com.fm.Tools.Process.Diversion.TypeDiversionProcess;
import com.fm.Tools.Process.WindowAll.*;
import com.fm.Tools.SendToExternal.SendToDorisTool;
import com.fm.Tools.SendToExternal.SendToKafkaTool;
import com.fm.Tools.SequenceBased.DataWithLocalDateTimeSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.ZoneId;
import java.util.*;

/**
 * <h1>实时排名(实现每个type的行为或结果排名)</h1>
 * <h2>可以说明的问题：</h2>
 * <table>
 *     <tr>
 *         <th>问题编号</th>
 *         <th>问题描述</th>
 *     </tr>
 *  <tr>
 *      <td>1</td>
 *      <td>当前用户关注什么商品（搜索）</td>
 *   </tr>
 *     <tr>
 *      <td>2</td>
 *      <td>用户偏好与市场趋势：长期点击排名反映真实需求（点击）</td>
 *      </tr>
 *     <tr>
 *      <td>3</td>
 *      <td>营销活动效果：通过分析订单、购买等排名，确定哪些商品被有效营销，哪些没有</td>
 *      </tr>
 *     <tr>
 *      <td>4</td>
 *      <td>商品潜力挖掘：识别 “黑马” 商品</td>
 *      </tr>
 *     <tr>
 *      <td>5</td>
 *      <td>商品 / 页面问题诊断：点击量与转化的 “反差” 暴露问题（点击量高但下单量少）</td>
 *      </tr>
 *     <tr>
 *      <td>6</td>
 *      <td>系统流量优化：识别高负载商品，避免卡顿</td>
 *      </tr>
 *  </tr>
 * </table>
 *
 * <h2>传入的数据(来源于Kafka)：</h2>
 * <p>JSON格式：</p>
 * <pre>
 * <p>TopNTypeMainData:</p>
 * {
 *     "type" -> 类型
 *     "username" -> 用户名（不一定有）
 *     "userIP" -> 用户的IP（绝对存在）
 *     "time" -> 时间
 *     "orderId" -> 订单ID
 *     "keyword" -> 关键词
 *     "message" -> 详情消息
 *     "data" -> List类型 -> 数据
 * }
 * <p>CommodityRankData:</p>
 * {
 *     "goodsId" -> 商品ID
 *     "quantity" -> 购买数量（默认1）
 *     "price" -> 商品价格
 *     "imageUrl" -> 商品图片URL路径
 *     "category" -> 商品类别
 *     "brand" -> 商品品牌
 *     "spec" -> 规格样式
 *     "sold" -> 商品销售量
 *     "isAD" -> 是否为广告  1为广告；0为非广告
 * }
 * </pre>
 *
 * <h2>输出的数据</h2>
 * <p>JSON格式:</p>
 * <pre>
 *     {
 *         暂定
 *     }
 * </pre>
 */
public class MainRealTimeRankingTopN {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Integer parallelism = 1;

    // 设置Json反序列化器
    private static final DeserializationSchema<TopNTypeMainData> jsonDeserializer = new JsonDeserializationSchema<>(TopNTypeMainData.class);
    // 设置Json系列化器
    private static final SerializationSchema<DataList<TopNData>> jsonSerializer = new JsonSerializationSchema<>();

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.setString(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(parallelism); // 为了方便查看结果，将并行度设置为1 测试时使用

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig config = env.getCheckpointConfig();
        // 启用未对齐检查点
        config.enableUnalignedCheckpoints();

        //  接收
        KafkaSource<TopNTypeMainData> kafkaSource = KafkaSource.<TopNTypeMainData>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("Goods-TopN-RealTime")
                .setGroupId("RealTime")
                .setValueOnlyDeserializer(jsonDeserializer)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        //  乱序处理
        DataStreamSource<TopNTypeMainData> realTimeRankData = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<TopNTypeMainData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((rankData, recordTimestamp) -> rankData.getTime()
                                .atZone(ZoneId.of("Asia/Shanghai"))
                                .toInstant()
                                .toEpochMilli())
                        .withIdleness(Duration.ofSeconds(10)),
                "Goods-TopN"
        );

//        realTimeRankData.print("奥特曼：");

        //  分流
        SingleOutputStreamOperator<TopNTypeMainData> afterDiversionDataByType = realTimeRankData.process(new TypeDiversionProcess());

        SingleOutputStreamOperator<RecommendationSystemGenuineData> process = realTimeRankData
                .filter(event -> event != null && event.getUserIP() != null)
                .keyBy(t -> t.getUsername() != null ? t.getUsername() : "SESSION_" + t.getUserIP())
                .process(new RecommendationSystemDataKeyedProcess());

        SendToKafkaTool<RecommendationSystemGenuineData> recommendationSystemToKafka = SendToKafkaTool.<RecommendationSystemGenuineData>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("Recommendation-SystemGenuineData")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("orderAmount-topN")
                .serializationSchema(new DataWithLocalDateTimeSerializationSchema<>())
                .build();

        recommendationSystemToKafka.sendToKafka(process, parallelism);




        //  获取各个侧输出流
        //  ID
        SideOutputDataStream<TopNTypeMainData> idSideOutput = afterDiversionDataByType.getSideOutput(TypeDiversionProcess.ID_DATA);
        //  SEARCH
        SideOutputDataStream<TopNTypeMainData> searchSideOutput = afterDiversionDataByType.getSideOutput(TypeDiversionProcess.SEARCH_DATA);
        //  CART
        SideOutputDataStream<TopNTypeMainData> cartSideOutput = afterDiversionDataByType.getSideOutput(TypeDiversionProcess.CART_DATA);
        //  ORDER
        SideOutputDataStream<TopNTypeMainData> orderSideOutput = afterDiversionDataByType.getSideOutput(TypeDiversionProcess.ORDER_DATA);
        //  PAY
        SideOutputDataStream<TopNTypeMainData> paySideOutput = afterDiversionDataByType.getSideOutput(TypeDiversionProcess.PAY_DATA);

        //************************ 1. 商品维度分析 *****************************//
        //  不同流做不同处理
        //  ID
        idTopNHandle(idSideOutput);

        //  SEARCH
        searchTopNHandle(searchSideOutput);

        //  CART
        SingleOutputStreamOperator<TopNTypeMainData> filterCartData = cartSideOutput
                .filter(data -> data.getMessage() != null && data.getData() != null);

        cartTopNHandle(filterCartData, "add", "CART-TopN-ADD-Result: ");

        cartTopNHandle(filterCartData, "delete", "CART-TopN-DELETE-Result: ");

        //  ORDER
        SingleOutputStreamOperator<TopNTypeMainData> createOrderData = orderSideOutput.filter(data -> data.getMessage().equals("创建订单"))
                .filter(data -> data.getData() != null && !data.getData().isEmpty());

        createOrderTopNHandle(createOrderData);

        //  种类与品牌
        DataStream<TopNData> allBehaviorData = idSideOutput.flatMap(new UniversalFlatMap()).union(cartSideOutput.filter(data -> data.getMessage().equals("add")).flatMap(new UniversalFlatMap()), createOrderData.flatMap(new TargetOrderDataFlatMap()));

        //  种类畅销排名
        categoryTopNHandle(allBehaviorData);

        //  品牌
        brandTopNHandle(allBehaviorData);

        //************************ 2. 用户行为分析 *****************************//
        //  地域分布于活跃用户名字并进行排名
        usernameByRegion(realTimeRankData);

        //  行为路径分析
        //  1. 浏览→加购→下单→支付 转化漏斗
        //  处理转换
        SingleOutputStreamOperator<BehaviorAnalysisResult> analysisResult = realTimeRankData
                .filter(data -> data.getUsername() != null && !data.getUsername().isEmpty())
                .map(data -> {
                    String username = data.getUsername();
                    String behaviorType = data.getType();
                    Long timestamp = data.getTime().atZone(ZoneId.of("Asia/Shanghai")).toInstant().toEpochMilli();
                    return new UserBehavior(username, behaviorType, timestamp);
                })
                .keyBy(UserBehavior::getUsername)
                .process(new KeyedProcess());

        //  处理转化漏斗
        convertFunnelSend(analysisResult);

        //  每步用时
        intervalSend(analysisResult);

        //************************ 3. 交易实时监控 *****************************//
        //  1. 订单创建成功率
        SingleOutputStreamOperator<SuccessRateWindowResult> createOrderSuccessRate = orderSideOutput
                .keyBy(TopNTypeMainData::getUserIP)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SuccessRateAgg(), new SuccessRateWindow());

        createOrderSuccessRate.print("订单创建成功率统计结果：\n");

        GeneralSandDataRate(createOrderSuccessRate, "createOrder");


        //  2. 实时订单金额TopN
        SingleOutputStreamOperator<List<Amount>> createOrderProcessData = orderSideOutput
                .filter(data -> data.getMessage() != null && data.getMessage().equals("创建订单"))
                .filter(data -> data.getData() != null && !data.getData().isEmpty())
                .keyBy(TopNTypeMainData::getUsername)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new OrderAmountTopNAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .process(new OrderAmountTopNProcess(10));

        createOrderProcessData.print("创建订单实时金额TopN结果：\n");

        createOrderAmount(createOrderProcessData);


        //  3. 支付状态实时追踪
        SingleOutputStreamOperator<Map<String, String>> payStatus = paySideOutput
                .keyBy(TopNTypeMainData::getUserIP)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new PayStatusAgg());

        payStatus.print("PAY-STATUS-Result: \n");

        payStatusSend(payStatus);


        //  4. 购物车放弃率
        SingleOutputStreamOperator<SuccessRateWindowResult> cartAbandonRate = cartSideOutput
                .filter(data -> !data.getMessage().equals("update"))
                .keyBy(TopNTypeMainData::getUserIP)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new SuccessRateAgg(SuccessRateAgg.SuccessRateType.CART), new SuccessRateWindow(SuccessRateWindow.RateType.ABANDON_CART));

        cartAbandonRate.print("购物车放弃率统计结果：\n");

        GeneralSandDataRate(cartAbandonRate, "cartAbandon");

        //************************ 4. 广告效果监控 *****************************//
        //  1. 广告商品点击率
        SingleOutputStreamOperator<SuccessRateWindowResult> adClickRate = idSideOutput
                .filter(data -> data.getData() != null && !data.getData().isEmpty())
                .keyBy(TopNTypeMainData::getUserIP)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new ClickADRateAgg(), new SuccessRateWindow(SuccessRateWindow.RateType.CLICK_AD));

        adClickRate.print("广告商品点击率统计结果：\n");

        GeneralSandDataRate(adClickRate, "adClick");

        //  2. 广告转化率
        SingleOutputStreamOperator<SuccessRateWindowResult> adChangeRate = orderSideOutput
                .filter(data -> data.getData() != null && !data.getData().isEmpty())
                .keyBy(TopNTypeMainData::getUserIP)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new ClickADRateAgg(), new SuccessRateWindow(SuccessRateWindow.RateType.CHANGE_AD));

        adChangeRate.print("广告转化率统计结果：\n");

        GeneralSandDataRate(adChangeRate, "adChange");

        env.execute("RealTimeRanking");

    }

    private static void payStatusSend(SingleOutputStreamOperator<Map<String, String>> payStatus) {

        SendToKafkaTool<Map<String, String>> sendToKafkaTool = SendToKafkaTool.<Map<String, String>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("payStatus")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("payStatus")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        sendToKafkaTool.sendToKafka(payStatus, parallelism);

        SingleOutputStreamOperator<String> jsonPayStatus = payStatus.flatMap((Map<String, String> data, Collector<String> collector) -> {
                    Set<Map.Entry<String, String>> entrySet = data.entrySet();
                    for (Map.Entry<String, String> entry : entrySet) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        String json = "{" +
                                "\"orderId\":\"" + key + "\"," +
                                "\"status\":\"" + value + "\"," +
                                "}";
                        collector.collect(json);
                    }
                })
                .returns(String.class);

        SendToDorisTool payStatusSendToDoris = new SendToDorisTool(
                "FireflyMall.payStatus",
                SendToDorisTool.DATATYPE.JSON,
                "pay-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        payStatusSendToDoris.sendDataToDoris(jsonPayStatus, parallelism);
    }

    private static void createOrderAmount(SingleOutputStreamOperator<List<Amount>> createOrderProcessData) {

        SendToKafkaTool<List<Amount>> sendToKafkaTool = SendToKafkaTool.<List<Amount>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("orderAmountTopN")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("orderAmount-topN")
                .serializationSchema(new DataWithLocalDateTimeSerializationSchema<>())
                .build();

        sendToKafkaTool.sendToKafka(createOrderProcessData, parallelism);

        SingleOutputStreamOperator<String> jsonCreateOrderAmount = createOrderProcessData.map(Object::toString);

        SendToDorisTool amountTopNSendToDoris = new SendToDorisTool(
                "FireflyMall.orderAmountTopN",
                SendToDorisTool.DATATYPE.JSON,
                "amount-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        amountTopNSendToDoris.sendDataToDoris(jsonCreateOrderAmount, parallelism);
    }

    private static void GeneralSandDataRate(SingleOutputStreamOperator<SuccessRateWindowResult> dataRate, String type) {

        SendToKafkaTool<SuccessRateWindowResult> sendToKafkaTool = SendToKafkaTool.<SuccessRateWindowResult>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic(type + "Rate")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix(type + "-rate")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        sendToKafkaTool.sendToKafka(dataRate, parallelism);

        SingleOutputStreamOperator<String> stringJsonDataRate = dataRate.map(SuccessRateWindowResult::toString);

        SendToDorisTool createOrderSuccessRateSendToDoris = new SendToDorisTool(
                "FireflyMall.typeRate",
                SendToDorisTool.DATATYPE.JSON,
                "Rate-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        createOrderSuccessRateSendToDoris.sendDataToDoris(stringJsonDataRate, parallelism);
    }

    private static void intervalSend(SingleOutputStreamOperator<BehaviorAnalysisResult> analysisResult) {
        SingleOutputStreamOperator<IntervalResult> intervalResult = analysisResult
                .filter(result -> "INTERVAL".equals(result.getAnalysisType()))
                .keyBy(BehaviorAnalysisResult::getDimension)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new IntervalAgg(), new IntervalWindow());

        intervalResult.print("每步用时统计结果：\n");

        SendToKafkaTool<IntervalResult> intervalSendToKafka = SendToKafkaTool.<IntervalResult>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("intervalTime")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("interval-time")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        intervalSendToKafka.sendToKafka(intervalResult, parallelism);

        SingleOutputStreamOperator<String> stringInterval = intervalResult.map(IntervalResult::toString).filter(data -> !data.isEmpty());

        SendToDorisTool intervalSendToDoris = new SendToDorisTool(
                "FireflyMall.intervalTime",
                SendToDorisTool.DATATYPE.JSON,
                "Interval-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        intervalSendToDoris.sendDataToDoris(stringInterval, parallelism);
    }

    private static void convertFunnelSend(SingleOutputStreamOperator<BehaviorAnalysisResult> analysisResult) {
        SingleOutputStreamOperator<FunnelResult> convertFunnelResult = analysisResult
                .filter(result -> "FUNNEL".equals(result.getAnalysisType()))
                .keyBy(BehaviorAnalysisResult::getDimension)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new ConvertAgg(), new ConvertWindow());

        convertFunnelResult.print("漏斗统计结果：\n");

        SendToKafkaTool<FunnelResult> convertFunnelSendToKafka = SendToKafkaTool.<FunnelResult>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("convertCount")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("convert-count")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        convertFunnelSendToKafka.sendToKafka(convertFunnelResult, parallelism);

        SingleOutputStreamOperator<String> stringConvert = convertFunnelResult.map(FunnelResult::toString)
                .filter(result -> !result.isEmpty());

        SendToDorisTool convertCountSendToDoris = new SendToDorisTool(
                "FireflyMall.convertCount",
                SendToDorisTool.DATATYPE.JSON,
                "Convert-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        convertCountSendToDoris.sendDataToDoris(stringConvert, parallelism);
    }

    private static void usernameByRegion(DataStreamSource<TopNTypeMainData> realTimeRankData) {
        SingleOutputStreamOperator<DataList<userIpAndUsername>> userRegionProcessData = realTimeRankData
                .flatMap((TopNTypeMainData data, Collector<Tuple2<String, String>> collect) -> {
                    String userIP = data.getUserIP();
                    String userName = data.getUsername() != null ? data.getUsername() : "";
                    collect.collect(new Tuple2<>(userIP, userName));
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new UserIPCountAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .process(new UserIPCountTopN(100));

        userRegionProcessData.print("USER-IP-TopN-Result: \n");

        SendToKafkaTool<DataList<userIpAndUsername>> userIpRegionSendToKafka = SendToKafkaTool.<DataList<userIpAndUsername>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("userIpRegionTopN")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("userIpRegion-topN")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        userIpRegionSendToKafka.sendToKafka(userRegionProcessData, parallelism);

        SingleOutputStreamOperator<String> userIpRegionData = userRegionProcessData
                .flatMap((DataList<userIpAndUsername> dataList, Collector<String> collect) -> {
                    List<userIpAndUsername> userIpAndUsernameList = dataList.getData();

                    for (userIpAndUsername userIpAndUsername : userIpAndUsernameList) {

                        String userIP = userIpAndUsername.getUserIP();
                        String windowStart = dataList.getWindowStart();
                        String windowEnd = dataList.getWindowEnd();
                        Integer topN = userIpAndUsername.getTopN();
                        Integer count = userIpAndUsername.getCount();
                        for (String username : userIpAndUsername.getUsernameList()) {
                            UserIpRegionData userData = new UserIpRegionData(
                                    userIP,
                                    username,
                                    windowStart,
                                    windowEnd,
                                    count,
                                    topN
                            );
                            collect.collect(userData.toString());
                        }
                    }
                })
                .returns(String.class);

        SendToDorisTool userIpRegionSendToDoris = new SendToDorisTool(
                "FireflyMall.userIpRegionTopN",
                SendToDorisTool.DATATYPE.JSON,
                "Region-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        userIpRegionSendToDoris.sendDataToDoris(userIpRegionData, parallelism);


    }

    private static void brandTopNHandle(DataStream<TopNData> allBehaviorData) {
        SingleOutputStreamOperator<DataList<CategoryOrBrandTopNData>> brandProcessData = allBehaviorData
                .keyBy(TopNData::getBrand)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new CategoryAndBrandAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .process(new TargetCategoryAndBrandTopN(5));

        brandProcessData.print("BRAND-TopN-Result: \n");

        SendToKafkaTool<DataList<CategoryOrBrandTopNData>> brandSendToKafka = SendToKafkaTool.<DataList<CategoryOrBrandTopNData>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("brandTopN")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("brand-topN")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        brandSendToKafka.sendToKafka(brandProcessData, parallelism);

        SendToDorisTool categorySendToDoris = new SendToDorisTool(
                "FireflyMall.categoryOrBrandTopN",
                SendToDorisTool.DATATYPE.JSON,
                "category-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        TargetCategoryAndBrandSend(brandProcessData, categorySendToDoris);

    }

    private static void categoryTopNHandle(DataStream<TopNData> allBehaviorData) {
        SingleOutputStreamOperator<DataList<CategoryOrBrandTopNData>> categoryProcessData = allBehaviorData
                .keyBy(TopNData::getCategory)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new CategoryAndBrandAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .process(new TargetCategoryAndBrandTopN(5));

        categoryProcessData.print("CATEGORY-TopN-Result: \n");

        SendToKafkaTool<DataList<CategoryOrBrandTopNData>> categorySendToKafka = SendToKafkaTool.<DataList<CategoryOrBrandTopNData>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("categoryTopN")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("category-topN")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        categorySendToKafka.sendToKafka(categoryProcessData, parallelism);


        SendToDorisTool categorySendToDoris = new SendToDorisTool(
                "FireflyMall.categoryOrBrandTopN",
                SendToDorisTool.DATATYPE.JSON,
                "category-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        TargetCategoryAndBrandSend(categoryProcessData, categorySendToDoris);

    }

    private static void createOrderTopNHandle(SingleOutputStreamOperator<TopNTypeMainData> createOrderData) {
        SingleOutputStreamOperator<DataList<TopNData>> createOrderProcessData = createOrderData.flatMap(new TargetOrderDataFlatMap())
                .returns(TopNData.class)
                .keyBy(TopNData::getGoodsId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountTargetOrderAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new topNProcessTargetOrderTool(5));

        createOrderProcessData.print("CREATE-ORDER-TopN-Result: \n");

        // 发送到kafka
        SendToKafkaTool<DataList<TopNData>> createOrderSendToKafka = SendToKafkaTool.<DataList<TopNData>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("createOrderTopN")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("createOrder-topN")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();
        createOrderSendToKafka.sendToKafka(createOrderProcessData, parallelism);


        SendToDorisTool cartSendToDoris = new SendToDorisTool(
                "FireflyMall.createOrderTopN",
                SendToDorisTool.DATATYPE.JSON,
                "createOrder-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        generalSend(createOrderProcessData, cartSendToDoris, "createOrder");

    }

    private static void cartTopNHandle(SingleOutputStreamOperator<TopNTypeMainData> filterCartData, String type, String sinkIdentifier) {

        SingleOutputStreamOperator<DataList<TopNData>> cartProcessData = filterCartData.filter(data -> data.getMessage().equals(type))
                .flatMap(new UniversalFlatMap())
                .returns(TopNData.class)
                .keyBy(TopNData::getGoodsId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new topNProcessTool(5));

        cartProcessData.print(sinkIdentifier + "\n");
        // 发送到kafka
        SendToKafkaTool<DataList<TopNData>> cartSendToKafka = SendToKafkaTool.<DataList<TopNData>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("cart" + type)
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("cart-topN" + type)
                .serializationSchema(new JsonSerializationSchema<>())
                .build();
        cartSendToKafka.sendToKafka(cartProcessData, parallelism);


        SendToDorisTool cartSendToDoris = new SendToDorisTool(
                "FireflyMall.cartTopN",
                SendToDorisTool.DATATYPE.JSON,
                "cartTopN-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        generalSend(cartProcessData, cartSendToDoris, type);

    }

    private static void searchTopNHandle(SideOutputDataStream<TopNTypeMainData> searchSideOutput) {
        SingleOutputStreamOperator<DataList<TopNDataTargetSearch>> searchProcessData = searchSideOutput
                .filter(data -> data.getKeyword() != null && data.getData() != null)
                .flatMap((TopNTypeMainData searchData, Collector<Tuple3<String, List<CommodityRankData>, Integer>> collector) -> {
                    List<CommodityRankData> commodityList = searchData.getData();
                    String keyword = searchData.getKeyword();
                    collector.collect(new Tuple3<>(keyword, commodityList, 1));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, List<CommodityRankData>, Integer>>() {
                }))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .aggregate(new CountTargetSearchAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)))
                .process(new topNTargetSearchProcessTool(5));

        searchProcessData.print("SEARCH-TopN-Result: \n");


        // 发送到kafka
        SendToKafkaTool<DataList<TopNDataTargetSearch>> searchSendToKafka = SendToKafkaTool.<DataList<TopNDataTargetSearch>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("searchTopN")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("search-topN-producer")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        searchSendToKafka.sendToKafka(searchProcessData, parallelism);


        SendToDorisTool searchSendToDoris = new SendToDorisTool(
                "FireflyMall.searchTopN", // 库.表
                SendToDorisTool.DATATYPE.JSON, // 数据类型
                "searchTopN-", // labelPrefix：标签前缀
                10000, // batchSize：攒够10000条就刷写（极小值，快速输出）
                1000, // BatchIntervalMs：1秒兜底刷写
                1, // maxRetries：减少重试次数（避免重试阻塞）
                10485760, // bufferFlushMaxBytes：10MB刷写
                1 // bufferCount：1个缓冲区
        );

        SingleOutputStreamOperator<String> stringSearchTopNData = searchProcessData
                .flatMap((DataList<TopNDataTargetSearch> dataList, Collector<String> out) -> {
                    List<TopNDataTargetSearch> topNDataList = dataList.getData();
                    if (topNDataList == null || topNDataList.isEmpty()) {
                        return;
                    }
                    for (TopNDataTargetSearch topNData : topNDataList) {
                        String keyword = topNData.getKeyword();
                        List<CommodityRankData> commodityList = topNData.getSearchList();
                        Integer count = topNData.getCount();
                        Integer topN = topNData.getTopN();
                        String windowStart = topNData.getWindowStart();  // 获取窗口开始时间
                        String windowEnd = topNData.getWindowEnd();      // 获取窗口结束时间

                        for (CommodityRankData commodityRankData : commodityList) {
                            String json = generateJson(commodityRankData, keyword, count, topN, windowStart, windowEnd);
                            out.collect(json);
                        }
                    }
                })
                .returns(String.class);

        searchSendToDoris.sendDataToDoris(stringSearchTopNData, parallelism);

    }

    private static void idTopNHandle(SideOutputDataStream<TopNTypeMainData> idSideOutput) {
        SingleOutputStreamOperator<DataList<TopNData>> idProcessData = idSideOutput
                .flatMap(new UniversalFlatMap())
                .returns(TopNData.class)
                .keyBy(TopNData::getGoodsId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new CountAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new topNProcessTool(10));

        idProcessData.print("ID-TopN-Result: \n");

        //  将结果发送到Kafka中
        SendToKafkaTool<DataList<TopNData>> idSendToKafka = SendToKafkaTool.<DataList<TopNData>>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("idTopN")
                .serializationSchema(jsonSerializer)
                .transactionalIdPrefix("id-topN-")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        idSendToKafka.sendToKafka(idProcessData, parallelism);

        // 发送到Doris
        SendToDorisTool idSendToDoris = new SendToDorisTool(
                "FireflyMall.idTopN", // 库.表
                SendToDorisTool.DATATYPE.JSON, // 数据类型
                "idTopN-", // labelPrefix：标签前缀
                10000, // batchSize：攒够10000条就刷写（极小值，快速输出）
                1000, // BatchIntervalMs：1秒兜底刷写
                1, // maxRetries：减少重试次数（避免重试阻塞）
                10485760, // bufferFlushMaxBytes：10MB刷写
                1 // bufferCount：1个缓冲区
        );

        generalSend(idProcessData, idSendToDoris, "id");

    }

    private static void TargetCategoryAndBrandSend(SingleOutputStreamOperator<DataList<CategoryOrBrandTopNData>> categoryProcessData, SendToDorisTool categorySendToDoris) {

        SingleOutputStreamOperator<String> categoryOrBrandJson = categoryProcessData.
                flatMap((DataList<CategoryOrBrandTopNData> data, Collector<String> out) -> {
                    List<CategoryOrBrandTopNData> categoryOrBrandTopNDataList = data.getData();
                    if (categoryOrBrandTopNDataList == null || categoryOrBrandTopNDataList.isEmpty()) {
                        return;
                    }
                    String windowStart = data.getWindowStart();  // 获取窗口开始时间
                    String windowEnd = data.getWindowEnd();      // 获取窗口结束时间
                    for (CategoryOrBrandTopNData categoryOrBrandTopNData : categoryOrBrandTopNDataList) {
                        int count = categoryOrBrandTopNData.getCount();
                        int topN = categoryOrBrandTopNData.getTopN();
                        String type = categoryOrBrandTopNData.getCategoryOrBrand();
                        List<TopNData> topNDataList = categoryOrBrandTopNData.getTopNData();
                        for (TopNData topNData : topNDataList) {
                            TargetCategoryAndBrandSendData sendData = new TargetCategoryAndBrandSendData(
                                    type,
                                    topNData.getGoodsId(),
                                    topNData.getUserId(),
                                    topNData.getKeyword(),
                                    topNData.getMessage(),
                                    topNData.getSold(),
                                    topNData.getCategory(),
                                    topNData.getBrand(),
                                    topNData.getAD(),
                                    windowStart,
                                    windowEnd,
                                    count,
                                    topN
                            );
                            out.collect(sendData.toString());
                        }
                    }
                })
                .returns(String.class)
                .filter(Objects::nonNull);

        categorySendToDoris.sendDataToDoris(categoryOrBrandJson, parallelism);

    }

    private static void generalSend(SingleOutputStreamOperator<DataList<TopNData>> processData, SendToDorisTool idSendToDoris, String type) {
        SingleOutputStreamOperator<String> stringIdTopNData = processData.flatMap((DataList<TopNData> data, Collector<String> out) -> {
                    List<TopNData> topNDataList = data.getData();
                    if (topNDataList == null || topNDataList.isEmpty()) {
                        return;
                    }
                    // 从DataList中获取窗口开始时间和结束时间
                    String windowStart = data.getWindowStart();
                    String windowEnd = data.getWindowEnd();

                    for (TopNData topNData : topNDataList) {
                        TopNDataAndTime newTopNData = new TopNDataAndTime(
                                topNData.getGoodsId(),
                                topNData.getUserId(),
                                topNData.getKeyword(),
                                topNData.getMessage(),
                                topNData.getSold(),
                                topNData.getCategory(),
                                topNData.getBrand(),
                                type,
                                topNData.getAD(),
                                topNData.getCount(),
                                windowStart,
                                windowEnd,
                                topNData.getTopN()
                        );
                        out.collect(newTopNData.toString());
                    }
                })
                .returns(String.class)
                .filter(Objects::nonNull);

        idSendToDoris.sendDataToDoris(stringIdTopNData, parallelism);
    }

    private static String generateJson(CommodityRankData data, String keyword, Integer count, Integer topN, String windowStart, String windowEnd) throws JsonProcessingException {
        // 创建JSON对象
        ObjectNode jsonNode = objectMapper.createObjectNode();

        jsonNode.put("keyword", keyword);
        jsonNode.put("id", data.getId());
        jsonNode.put("quantity", data.getQuantity());
        jsonNode.put("price", data.getPrice());
        jsonNode.put("image", data.getImage() != null ? data.getImage() : "");
        jsonNode.put("category", data.getCategory() != null ? data.getCategory() : "");
        jsonNode.put("brand", data.getBrand() != null ? data.getBrand() : "");

        // spec作为JSON字符串
        String specJson = "{}";
        if (data.getSpec() != null) {
            specJson = objectMapper.writeValueAsString(data.getSpec());
        }
        jsonNode.put("spec", specJson);  // 这里会自动处理转义

        jsonNode.put("sold", data.getSold());
        jsonNode.put("isAD", data.getIsAD());
        jsonNode.put("count", count);
        jsonNode.put("topN", topN);
        jsonNode.put("windowStart", windowStart);
        jsonNode.put("windowEnd", windowEnd);

        return objectMapper.writeValueAsString(jsonNode);
    }

}
