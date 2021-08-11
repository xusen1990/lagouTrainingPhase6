package com.lagou.kafka.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer1 {

    public static void main(String[] args) {

        Map<String, Object> configs = new HashMap<>();
        // 设置连接Kafka的初始连接用到的服务器地址
        // 如果是集群，则可以通过此初始连接发现集群中的其他broker
        configs.put("bootstrap.servers", "10.69.78.49:9092");
        // 设置key的序列化器
        configs.put("key.serializer", IntegerSerializer.class);
        // 设置value的序列化器
        configs.put("value.serializer", StringSerializer.class);

        configs.put("acks", "all");
        configs.put("reties", "3");

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(configs);

        // 用于设置用户自定义的消息头字段
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("biz.name","producer.demo".getBytes()));

        ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(
                "topic_1",  // 主题名称
                0,       // 分区编号，现在只有一个分区，所以是0
                0,           // 数字作为key
                "hello lagou 0", // 字符串作为value
                 headers           // 自定义的消息头
        );
        // 消息的同步确认
        final Future<RecordMetadata> future = producer.send(record);
        try {
            final RecordMetadata metadata = future.get();

            System.out.println("消息的主题：" + metadata.topic());
            System.out.println("消息的分区号：" + metadata.partition());
            System.out.println("消息的偏移量:" + metadata.offset());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        // 关闭生产者
        producer.close();
    }
}
