package com.zxelec.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * ConsumerService
 *
 * @author vimicro
 * @date 2021/2/4
 */
@Service
@Slf4j
public class ConsumerService {

    private static final String TOPIC_NAME = "part3-topic";
    private static final String GROUP_ID = "group.demo";

    /**
     * 接收消息
     */
    @KafkaListener(id = "", topics = TOPIC_NAME, groupId = GROUP_ID)
    public void listener(ConsumerRecord<String, String> record) {
        System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
    }
}
