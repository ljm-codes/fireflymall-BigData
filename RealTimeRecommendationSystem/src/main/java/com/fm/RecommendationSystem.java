package com.fm;

import com.fm.ADS.RecommendedResults;
import com.fm.ADS.UserBehaviorHiveData;
import com.fm.ODS.DWM.SwitchGenuineData;
import com.fm.ODS.RecommendationSystemGenuineData;
import com.fm.Tools.Agg.UserBehaviorHistoryAgg;
import com.fm.Tools.FlatMap.SplitRecommendedGoodsListByWeight;
import com.fm.Tools.FlatMap.SwitchDataFlatMap;
import com.fm.Tools.FlatMap.ToHiveDataFlatMap;
import com.fm.Tools.FlatMap.ToJsonDataFlatMap;
import com.fm.Tools.Process.*;
import com.fm.Tools.Reduce.MergeRecommendedResultsReduce;
import com.fm.Tools.SendToExternal.SendToDorisTool;
import com.fm.Tools.SendToExternal.SendToKafkaTool;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneId;

/**
 * <h1>Flink实时推荐系统</h1>
 * <p>从kafka中获取数据：{@link RecommendationSystemGenuineData}</p>
 * <p>根据侧输出流将数据分为明确用户和匿名用户</p>
 * <p>明确用户将会使用 item-cf推荐算法</p>
 * <p>而匿名用户将会使用session-based推荐算法</p>
 * <p>最终将结果写入到kafka中：{@link RecommendedResults}</p>
 */
public class RecommendationSystem {

    //  json反序列器
    private static final DeserializationSchema<RecommendationSystemGenuineData> jsonDeserializer = new JsonDeserializationSchema<>(RecommendationSystemGenuineData.class);

    private static final int parallelism = 1;

    private static final String hiveName = "FireflyMallHive";
    private static final String database = "FireflyMall";
    private static final String hiveVersion = "3.1.3";

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration conf = new Configuration();

        conf.setString(RestOptions.BIND_PORT, "9090-9099");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //  配置HiveCatalog
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("create catalog " + hiveName + " with (" +
                " 'type'='hive'," +
                " 'hive-version'='" + hiveVersion + "'" +
                ")");

        tableEnv.useCatalog(hiveName);
        tableEnv.executeSql("use " + database);

        env.setParallelism(parallelism);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig config = env.getCheckpointConfig();
        // 启用未对齐检查点
        config.enableUnalignedCheckpoints();

        //  从Recommendation-SystemGenuineData主题中获取原始数据
        KafkaSource<RecommendationSystemGenuineData> kafkaSource = KafkaSource.<RecommendationSystemGenuineData>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("Recommendation-SystemGenuineData")
                .setGroupId("RecommendationSystem")
                .setValueOnlyDeserializer(jsonDeserializer)
                .build();

        //  乱序处理
        DataStreamSource<RecommendationSystemGenuineData> genuineData = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<RecommendationSystemGenuineData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) ->
                                element.getCreateTime()
                                        .atZone(ZoneId.of("Asia/Shanghai"))
                                        .toInstant()
                                        .toEpochMilli())
                        .withIdleness(Duration.ofSeconds(5)),
                "recommendationSystem"

        );

        genuineData.print("原始数据：");
        //  传入hive
        SingleOutputStreamOperator<UserBehaviorHiveData> mainStreamData = genuineData.flatMap(new ToHiveDataFlatMap())
                .returns(UserBehaviorHiveData.class)
                .process(new OrderPayStatusProcessFunction());// 分出order与payStatus

        SingleOutputStreamOperator<UserBehaviorHiveData> completeOrderData = mainStreamData.getSideOutput(OrderPayStatusProcessFunction.ORDER_PAY)
                .keyBy(UserBehaviorHiveData::getOrderId)
                .process(new OrderPayMatcherProcess());

        DataStream<UserBehaviorHiveData> userBehaviorHiveData = mainStreamData.union(completeOrderData);

        userBehaviorHiveData.print("处理后数据：");

        Table enrichedTable = tableEnv.fromDataStream(
                userBehaviorHiveData,
                Schema.newBuilder()
                        .column("userId", "STRING")
                        .column("type", "STRING")
                        .column("orderId", "STRING")
                        .column("payStatus", "STRING")
                        .column("keyword", "STRING")
                        .column("message", "STRING")
                        .column("goodsId", "BIGINT")
                        .column("quantity", "INT")
                        .column("price", "INT")
                        .column("image", "STRING")
                        .column("category", "STRING")
                        .column("brand", "STRING")
                        .column("spec", "STRING")
                        .column("sold", "INT")
                        .column("AD", "INT")
                        .column("createTime", "TIMESTAMP(3)")
                        .columnByExpression("dt", "DATE_FORMAT(createTime, 'yyyy-MM-dd')")
                        .build()
        );

        tableEnv.createTemporaryView("enriched_view", enrichedTable); //    创建临时视图
        tableEnv.executeSql("insert into UserBehaviorData select * from enriched_view"); //  插入到Hive表中

        SingleOutputStreamOperator<RecommendationSystemGenuineData> filteredGenuineData = genuineData.process(new UserDiversionTool());

        SideOutputDataStream<RecommendationSystemGenuineData> clarifyUserGenuineData = filteredGenuineData.getSideOutput(UserDiversionTool.USER);
        SideOutputDataStream<RecommendationSystemGenuineData> unclearUserGenuineData = filteredGenuineData.getSideOutput(UserDiversionTool.SESSION);

        SingleOutputStreamOperator<SwitchGenuineData> switchData = clarifyUserGenuineData
                .flatMap(new SwitchDataFlatMap()) //  进行转换
                .returns(SwitchGenuineData.class)
                .process(new SwitchDataProcess());

        // ============================  明确用户推荐（item-cf） ============================ //
        //  对switchData进行侧输出流处理
        SideOutputDataStream<SwitchGenuineData> idData = switchData.getSideOutput(SwitchDataProcess.ID); // 0.2
        SideOutputDataStream<SwitchGenuineData> searchData = switchData.getSideOutput(SwitchDataProcess.SEARCH); // 0.1
        SideOutputDataStream<SwitchGenuineData> cartData = switchData.getSideOutput(SwitchDataProcess.CART); // 0.3
        SideOutputDataStream<SwitchGenuineData> orderData = switchData.getSideOutput(SwitchDataProcess.ORDER); // 0.4

        SingleOutputStreamOperator<RecommendedResults> idRecommendedResults = UnifiedProcessRecommendedData(idData, 0.2);
        SingleOutputStreamOperator<RecommendedResults> searchRecommendedResults = UnifiedProcessRecommendedData(searchData, 0.1);
        SingleOutputStreamOperator<RecommendedResults> cartRecommendedResults = UnifiedProcessRecommendedData(cartData, 0.3);
        SingleOutputStreamOperator<RecommendedResults> orderRecommendedResults = UnifiedProcessRecommendedData(orderData, 0.4);

        //  合并所有推荐结果
        DataStream<RecommendedResults> allRecommendedResults = idRecommendedResults
                .union(searchRecommendedResults, cartRecommendedResults, orderRecommendedResults);

        SingleOutputStreamOperator<RecommendedResults> finalRecommendedResults = allRecommendedResults.keyBy(RecommendedResults::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new MergeRecommendedResultsReduce()); // 最终输出到kafka中，clarify主题

        SendToKafkaTool<RecommendedResults> clarifyDataToKafka = SendToKafkaTool.<RecommendedResults>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("clarifyUserRecommendData")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("clarify")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        clarifyDataToKafka.sendToKafka(finalRecommendedResults, parallelism);

        SingleOutputStreamOperator<String> jsonClarifyData = finalRecommendedResults.flatMap(new ToJsonDataFlatMap());

        SendToDorisTool clarifyDataToDoris = new SendToDorisTool(
                "FireflyMall.clarifyUserRecommendData",
                SendToDorisTool.DATATYPE.JSON,
                "mall-clarify-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        clarifyDataToDoris.sendDataToDoris(jsonClarifyData, parallelism);

        // ============================  不明确用户推荐（session-based/Session-based ItemCF） ============================ //
        SingleOutputStreamOperator<SwitchGenuineData> unclearSwitchData = unclearUserGenuineData
                .flatMap(new SwitchDataFlatMap())
                .returns(SwitchGenuineData.class)
                .process(new SwitchDataProcess());

        //  只存在id与search数据
        SideOutputDataStream<SwitchGenuineData> unclearIdData = unclearSwitchData.getSideOutput(SwitchDataProcess.ID);
        SideOutputDataStream<SwitchGenuineData> unclearSearchData = unclearSwitchData.getSideOutput(SwitchDataProcess.SEARCH);

        SingleOutputStreamOperator<RecommendedResults> processedIdData = unclearIdData
                .keyBy(SwitchGenuineData::getUserId)
                .process(new SessionBasedItemCFProcess());

        SingleOutputStreamOperator<RecommendedResults> processedSearchData = unclearSearchData
                .keyBy(SwitchGenuineData::getUserId)
                .process(new SessionBasedItemCFProcess(SessionBasedItemCFProcess.ProcessType.SEARCH));

        //  合并所有推荐结果
        DataStream<RecommendedResults> allUnclearRecommendedResults = processedIdData
                .union(processedSearchData);

        SingleOutputStreamOperator<RecommendedResults> finalUnclearRecommendedResults = allUnclearRecommendedResults.keyBy(RecommendedResults::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new MergeRecommendedResultsReduce());

        SendToKafkaTool<RecommendedResults> unclearDataToKafka = SendToKafkaTool.<RecommendedResults>builder()
                .bootstrapServers("node1:9092,node2:9092,node3:9092")
                .topic("unclearUserRecommendData")
                .deliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .transactionalIdPrefix("unclear")
                .serializationSchema(new JsonSerializationSchema<>())
                .build();

        unclearDataToKafka.sendToKafka(finalUnclearRecommendedResults, parallelism);

        SingleOutputStreamOperator<String> jsonUnclearData = finalUnclearRecommendedResults.flatMap(new ToJsonDataFlatMap());

        SendToDorisTool unclearDataToDoris = new SendToDorisTool(
                "FireflyMall.unclearUserRecommendData",
                SendToDorisTool.DATATYPE.JSON,
                "mall-unclear-",
                10000,
                1000,
                1,
                10485760,
                1
        );

        unclearDataToDoris.sendDataToDoris(jsonUnclearData, parallelism);

        env.execute("RealTimeRecommendationSystem");

    }

    private static SingleOutputStreamOperator<RecommendedResults> UnifiedProcessRecommendedData(SideOutputDataStream<SwitchGenuineData> data, double weight) {
        if (weight <= 0 || weight >= 1) {
            throw new IllegalArgumentException("weight must be greater than 0 and less than 1");
        }
        return data.keyBy(SwitchGenuineData::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UserBehaviorHistoryAgg())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new UserBehaviorHistoryProcess())
                .flatMap(new SplitRecommendedGoodsListByWeight(weight))
                .returns(RecommendedResults.class);
    }

}
