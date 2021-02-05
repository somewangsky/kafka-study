package com.zxelec.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * SendController
 *
 * @author vimicro
 * @date 2021/2/4
 */
@RestController
public class ProducerController {

    @Autowired
    @Qualifier(value = "defaultKafkaTemplate")
    private KafkaTemplate template;

    private static final String TOPIC_NAME = "part3-topic";

    /**
     * 简单的发送消息
     *
     * @param input
     * @return
     */
    @GetMapping("/send/{input}")
    public String sendToKafka(@PathVariable(value = "input") String input) {
        this.template.send(TOPIC_NAME, input);
        return "send success";
    }

    /**
     * 带事务的发送
     *
     * @param input
     * @return
     */
    @GetMapping("/send/transaction/{input}")
    public String sendToKafkaWithTransaction(@PathVariable(value = "input") String input) {
        // 事务的支持
        template.executeInTransaction(t -> {
            t.send(TOPIC_NAME, input);
            if ("error".equals(input)) {
                throw new RuntimeException("input is error!");
            }
            t.send(TOPIC_NAME, input + "other");
            return true;
        });
        return "send success";
    }

    /**
     * 带事务的注解发送
     *
     * @param input
     * @return
     */
    @Transactional(rollbackFor = RuntimeException.class)
    @GetMapping("/send/transaction/annotation/{input}")
    public String sendToKafkaWithTransactionAnnotation(@PathVariable(value = "input") String input) {
        this.template.send(TOPIC_NAME, input);
        if ("error".equals(input)) {
            throw new RuntimeException("input is error!");
        }
        template.send(TOPIC_NAME, input + "other");
        return "send success";
    }
}
