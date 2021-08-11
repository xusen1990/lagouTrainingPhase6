package com.lagou.kafka.contoller;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaAsyncProducerController {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @RequestMapping("send/async/{message}")
    public String sendASync(@PathVariable String message){

        ListenableFuture<SendResult<Integer, String>> future =
                kafkaTemplate.send("topic-spring-01", 0, 0, message);

        // 设置回调函数，异步等待broker端的返回结果
        future.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("发送消息失败:" + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                RecordMetadata recordMetadata = result.getRecordMetadata();
                System.out.println("发送消息成功:" + recordMetadata.topic()
                                   + "\t" + recordMetadata.partition()
                                   + "\t" + recordMetadata.offset());
            }
        });

        return "success";
    }
}
