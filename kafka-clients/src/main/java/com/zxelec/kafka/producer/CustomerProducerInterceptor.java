package com.zxelec.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

import java.util.Map;

/**
 * CustomerInterceptor
 * tip:异常会被try catch 不会传递
 * <p>
 * 使用场景：
 * 1、按照某个规则过滤掉不符合要求的消息
 * 2、修改消息的内容
 * 3、统计类需求
 *
 * @author vimicro
 * @date 2021/1/29
 */
public class CustomerProducerInterceptor implements ProducerInterceptor<String, String> {
    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        if (record.value().length() < 8) {
            throw new KafkaException("kafka exception");
        } else {
            String modifiedValue = "prefix1-" + record.value();
            return new ProducerRecord<String, String>(record.topic(), record.partition(),
                    record.timestamp(), record.key(), modifiedValue, record.headers());
        }
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率=" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}