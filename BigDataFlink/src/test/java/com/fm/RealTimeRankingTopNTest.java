package com.fm;

import com.fm.ODS.RealTimeRankData;
import com.fm.Tools.SequenceBased.RealTimeRankDataSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Properties;

class RealTimeRankingTopNTest {

    private static final Long[] goodsId = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};

    public static void main(String[] args) {

        try (KafkaProducer<String, RealTimeRankData> producer = getStringRealTimeRankDataKafkaProducer()) {
            producer.initTransactions();
            producer.beginTransaction();
            int commitSize = 100;
            int i = 0;
            do {
                RealTimeRankData rankData = new RealTimeRankData();
                rankData.setGoodsId(goodsId[(int) (Math.random() * goodsId.length)]);
                rankData.setUserIP("192.168.1." + (int) (Math.random() * 255));
                rankData.setClickTime(LocalDateTime.now());
                rankData.setAdvertisement((int) (Math.random() * 2));

                producer.send(new ProducerRecord<>(
                                "RealTimeRankData", rankData.getGoodsId().toString(), rankData
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

    private static KafkaProducer<String, RealTimeRankData> getStringRealTimeRankDataKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", RealTimeRankDataSerializer.class.getName());
//        properties.put("interceptor.classes", valueInterceptorDemo.class.getName());
//        properties.put("partitioner.class", partitionDemo.class.getName());
        properties.put("idempotent.enable", "true"); // 开启幂等性 ，默认值为false 若开启，acks必须为-1或all
        properties.put("acks", "all"); // 开启幂等性后，acks只能为-1或all，否则会抛出异常
        properties.put("transactional.id", "uniqueTransactionId"); // 唯一的事务ID
        properties.put("transaction.timeout.ms", "600000"); // 事务超时时间，默认值为60秒，这里设置为10分钟

        return new KafkaProducer<>(properties);
    }

}
