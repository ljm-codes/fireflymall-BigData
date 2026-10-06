package com.fm;

import com.fm.ADS.TopNData;
import com.fm.Tools.SequenceBased.TopNDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class TopNTest {
    public static void main(String[] args){

        Scanner sc = new Scanner(System.in);
        // 标记是否退出
        final AtomicBoolean isQuit = new AtomicBoolean(false);

        // 优雅关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("消费者即将关闭...");
            sc.close();
        }));

        // 启动一个独立线程监听控制台输入（避免阻塞消费逻辑）
        new Thread(() -> {
            while (!isQuit.get()) {
                if (sc.hasNextLine()) {
                    String input = sc.nextLine().trim();
                    if ("exit".equalsIgnoreCase(input)) {
                        isQuit.set(true);
                        System.out.println("收到退出指令，即将停止消费...");
                    }
                }
            }
        }).start();

        try (KafkaConsumer<String, TopNData> consumer = getKafkaConsumer()) {
            consumer.subscribe(Collections.singletonList("TopNData"));
            System.out.println("消费者已启动，输入 exit 退出...");

            // 循环消费，直到收到退出指令
            while (!isQuit.get()) {
                // 拉取 Kafka 数据（超时时间10秒）
                ConsumerRecords<String, TopNData> records = consumer.poll(Duration.ofSeconds(10));

                if (!records.isEmpty()) {
                    System.out.println("本次拉取到 " + records.count() + " 条数据");
                    records.forEach(record -> {
                        // 增加空值判断，排查反序列化问题
                        if (record.value() == null) {
//                            String rawBytes = new String(record.rawValue(), StandardCharsets.UTF_8);
                            System.out.println("警告：反序列化得到 null 数据，原始value字节：");
                        } else {
                            System.out.println("消费到数据：" + record.value());
                        }
                    });
                    // 批量提交 Offset
                    consumer.commitSync();
                    System.out.println("本次数据消费完成，Offset 已提交");
                }
            }

            System.out.println("消费者正常关闭");
        }

    }

    private static KafkaConsumer<String, TopNData> getKafkaConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", TopNDeserializer.class.getName());
        props.put("group.id", "kafka2");
        props.put("auto.offset.reset", "earliest");// 从最早的offset开始消费。 如果不设置，默认从最新的offset开始消费（latest）。
        props.put("enable.auto.commit", "false");// 手动提交offset。 如果不设置，默认自动提交offset。
        //  isolation.level 隔离级别  read_uncommitted 未提交读（默认） read_committed 已提交读
        // 为什么要设置隔离级别 -》 为了避免消费者读取到其他消费者正在处理的offset，导致数据不一致。
        // 未提交读：消费者可以读取到未提交的offset，包括其他消费者正在处理的offset。
        // 已提交读：消费者只能读取到已提交的offset，其他消费者正在处理的offset对其不可见。
//        props.put("isolation.level", "read_committed");
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");//  CooperativeStickyAssignor 合作式粘性分配器
        return new KafkaConsumer<>(props);
    }
}
