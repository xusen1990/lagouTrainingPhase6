package com.lagou.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MyConsumer1 {

    public static void main(String[] args) {

        Map<String, Object> configs = new HashMap<>();
        // 设置连接Kafka的初始连接用到的服务器地址
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.69.78.49:9092");
        // 使用常量代替手写的字符串，配置key的反序列化器
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        // 配置value的反序列化器
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 配置消费组ID
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_demo");
        // 如果找不到当前消费者的有效偏移量
        // earliest 自动重置到最开始
        // latest   直接重置到消息偏移量的最后一个
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(configs);

        // 先订阅再消费
        consumer.subscribe(Arrays.asList("topic_1"));

        // 如果主题中没有可以消费的消息，则该方法可以放到while循环中，每过3秒重新拉取一次
        // 如果还没有拉取到，过3秒再去拉取，防止while循环太密集的poll调用
        while(true){
            // 批量从主题的分区拉取消息
            final ConsumerRecords<Integer, String> consumerRecords = consumer.poll(3000);

            // 遍历本次从主题的分区拉取的批量消息
            for(ConsumerRecord<Integer,String> record : consumerRecords){
                System.out.println(record.topic() + "\t" +
                        record.partition() + "\t" +
                        record.offset() + "\t" +
                        record.key() + "\t" +
                        record.value());
            }
        }

        // 关闭消费者
        //consumer.close();
    }
}
