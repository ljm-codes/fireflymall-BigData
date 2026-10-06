package com.fm;

import com.fm.ODS.Main.CommodityRankData;
import com.fm.ODS.SearchRankData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.LocalDateTime;
import java.util.*;

public class SearchGoodsTopNTest {

    private static final Long[] goodsId = {100123L, 124552L, 314565L, 446598L, 544554L, 615641L, 744516L, 844516L, 951551L, 105652L};
    private static final String[] keywords = {"手机", "电脑", "鼠标", "键盘", "耳机", "显示屏", "显卡", "苹果", "梨子", "香蕉"};

    public static void main(String[] args) {

        try (KafkaProducer<String, SearchRankData<CommodityRankData>> producer = getStringSearchRankDataKafkaProducer()) {
            producer.initTransactions();
            producer.beginTransaction();
            int commitSize = 100;
            int i = 0;
            do {
                SearchRankData<CommodityRankData> rankData = new SearchRankData<>();
                rankData.setKeyword(keywords[(int)(Math.random() * keywords.length)]);
                rankData.setUsername("user" + (int)(Math.random() * 1000));
                rankData.setIp("192.168.1." + (int)(Math.random() * 255));
                rankData.setSearchTime(LocalDateTime.now());
                List<CommodityRankData> commodityRankDataList = new ArrayList<>();
                int rand = (int) (Math.random() * 10);
                for (int j = 0; j < rand; j++) {
                    CommodityRankData commodityRankData = new CommodityRankData();
                    commodityRankData.setId(goodsId[(int)(Math.random() * goodsId.length)]);
                    commodityRankData.setPrice((int)(Math.random() * 10000));
                    commodityRankData.setImage("https://img1.baidu.com/it/u=3422223442,2028824388&fm=253&fmt=auto&app=138&f=JPEG?w=500&h=500");
                    commodityRankData.setCategory("electronics");
                    commodityRankData.setBrand("Apple");
                    commodityRankData.setSpec(Collections.singletonMap("color", "black"));
                    commodityRankData.setSold((int)(Math.random() * 1000));
//                    commodityRankData.setAD(j % 2 == 0);
                    commodityRankDataList.add(commodityRankData);
                }
                rankData.setDataList(commodityRankDataList);

                producer.send(new ProducerRecord<>(
                                "Goods-Search-RealTime", rankData.getKeyword(), rankData
                        ), (recordMetadata, e) -> {
                            if (e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println("offset: " + recordMetadata.offset());
                            }
                        }
                );

                i++;
                if (i % commitSize == 0) {
                    producer.commitTransaction();
                    producer.beginTransaction();
                }
                Thread.sleep(100);
            } while (i < 5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static KafkaProducer<String, SearchRankData<CommodityRankData>> getStringSearchRankDataKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", JsonSerializer.class.getName());
//        properties.put("interceptor.classes", valueInterceptorDemo.class.getName());
//        properties.put("partitioner.class", partitionDemo.class.getName());
        properties.put("idempotent.enable", "true"); // 开启幂等性 ，默认值为false 若开启，acks必须为-1或all
        properties.put("acks", "all"); // 开启幂等性后，acks只能为-1或all，否则会抛出异常
        properties.put("transactional.id", "Search-Transaction-Id"); // 唯一的事务ID
        properties.put("transaction.timeout.ms", "600000"); // 事务超时时间，默认值为60秒，这里设置为10分钟

        return new KafkaProducer<>(properties);
    }

}
