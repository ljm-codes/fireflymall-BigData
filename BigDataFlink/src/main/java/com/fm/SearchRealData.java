package com.fm;


import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.SearchRankData;
import com.fm.Tools.Agg.UserSearchHistoryAggregate;
import com.fm.Tools.Process.WindowAll.RecommendCommodityTopN;
import com.fm.Tools.SequenceBased.JSON.JsonDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.ZoneId;


/**
 * <h1>搜索实时数据</h1>
 * <p>用于处理用户搜索的实时数据，包含搜索关键词、搜索时间、搜索用户等信息。</p>
 * <h2>可以用于：</h2>
 * <table>
 *     <tr>
 *         <th>序号</th>
 *         <th>描述</th>
 *     </tr>
 *     <tr>
 *         <td>1</td>
 *         <td>用户的喜好</td>
 *     </tr>
 *     <tr>
 *         <td>2</td>
 *         <td>广告是否有效推广</td>
 *     </tr>
 *     <tr>
 *         <td>3</td>
 *         <td>基于用户关键词来推荐商品</td>
 *     </tr>
 *     <tr>
 *         <td>...</td>
 *         <td>...</td>
 *     </tr>
 * </table>
 *
 * <h2>传入数据（Kafka）</h2>
 * <pre>
 *     CommodityRankData: {
 *          private Long goodsId;
 *          private Integer price;
 *          private String imageUrl;
 *          private String category;
 *          private String brand;
 *          private Map<String, Object> spec;
 *          private Integer sold;
 *          private boolean isAD;
 *     }
 * </pre>
 *
 * <h2>传出的数据</h2>
 * <pre>
 *      {
 *
 *      }
 * </pre>
 * */
public class SearchRealData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        CheckpointConfig config = env.getCheckpointConfig();
        config.enableUnalignedCheckpoints();

        // 设置Json反序列化器
        DeserializationSchema<SearchRankData<CommodityRankData>> jsonDeserializer = new JsonDeserializer();
        // 设置Json系列化器
//        SerializationSchema<DataList<TopNData>> jsonSerializer = new JsonSerializationSchema<>();

        KafkaSource<SearchRankData<CommodityRankData>> filterCommodityRankDataSource = KafkaSource.<SearchRankData<CommodityRankData>>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("Goods-Search-RealTime")
                .setGroupId("Goods-Search-Filter-RealTime")
                .setValueOnlyDeserializer(jsonDeserializer)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStreamSource<SearchRankData<CommodityRankData>> searchRankDataDataStreamSource = env.fromSource(
                filterCommodityRankDataSource,
                WatermarkStrategy.<SearchRankData<CommodityRankData>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((searchData, timestamp) ->
                                searchData.getSearchTime()
                                        .atZone(ZoneId.of("Asia/Shanghai"))
                                        .toInstant()
                                        .toEpochMilli()
                        )
                        .withIdleness(Duration.ofSeconds(5)),
                "Filter-Commodity-Rank-Source"
        );

        searchRankDataDataStreamSource
                .filter(data -> {
//                    System.out.println("消费到的对象：" + data);
//                    System.out.println("dataList是否为null：" + (data != null ? data.getDataList() == null : "对象本身是null"));
                    return data != null && data.getDataList() != null;
                })
                .flatMap((SearchRankData<CommodityRankData> searchRankData, Collector<Tuple3<String, String, CommodityRankData>> collector) -> {
                    String userId = searchRankData.getUsername() != null ? searchRankData.getUsername() : searchRankData.getIp();
                    for(CommodityRankData commodityRankData : searchRankData.getDataList()){
                        collector.collect(new Tuple3<>(
                                searchRankData.getKeyword(),
                                userId,
                                commodityRankData
                        ));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, CommodityRankData>>() {}))
                .keyBy(t -> t.f1)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .aggregate(new UserSearchHistoryAggregate())
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new RecommendCommodityTopN())
                .print("推荐结果：");

        env.execute("Real-time Search Recommendation");
    }
}
