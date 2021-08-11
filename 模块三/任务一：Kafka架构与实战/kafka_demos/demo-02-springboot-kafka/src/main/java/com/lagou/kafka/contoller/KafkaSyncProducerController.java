package com.lagou.kafka.contoller;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class KafkaSyncProducerController {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @RequestMapping("send/sync/{message}")
    public String sendSync(@PathVariable String message){

        ListenableFuture<SendResult<Integer, String>> future =
                kafkaTemplate.send("topic-spring-01", 0, 0, message);

        try {
            // 同步发送消息
            SendResult<Integer, String> sendResult = future.get();

            RecordMetadata recordMetadata = sendResult.getRecordMetadata();
            System.out.println(recordMetadata.topic() + "\t" + recordMetadata.partition() +
                               "\t" + recordMetadata.offset());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return "success";
    }
}
