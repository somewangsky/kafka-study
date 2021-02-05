package com.zxelec.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;

import java.util.Map;

/**
 * KafkaTemplateConfig
 *
 * @author vimicro
 * @date 2021/2/4
 */
@Configuration
public class KafkaTemplateConfig {
    @Autowired
    private KafkaProperties kafkaProperties;

    @Primary
    @Bean(name="defaultKafkaTemplate")
    public KafkaTemplate<String,Object> kafkaTemplate(@Qualifier("defaultKafkaProducerFactory") ProducerFactory<String,Object> kafkaProducerFactory,
                                             ProducerListener<String,Object> kafkaProducerListener,
                                             ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<String,Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setDefaultTopic(kafkaProperties.getTemplate().getDefaultTopic());
        return kafkaTemplate;
    }

    @Bean
    public ProducerListener<String,Object> kafkaProducerListener() {
        return new LoggingProducerListener<>();
    }

    @Primary
    @Bean(name="defaultKafkaProducerFactory")
    public ProducerFactory<String,Object> kafkaProducerFactory() {
        DefaultKafkaProducerFactory<String,Object> factory = new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties());
        String transactionIdPrefix = kafkaProperties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        return factory;
    }
    /**
     * 获取生产者工厂
     */
    @Bean(name="newKafkaProducerFactory")
    public ProducerFactory<String,Object> newProducerFactory() {
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        // 修改参数名称
        producerProperties.put(ProducerConfig.ACKS_CONFIG,"all");
        DefaultKafkaProducerFactory<String,Object> factory = new DefaultKafkaProducerFactory<>(
                producerProperties);
        String transactionIdPrefix = kafkaProperties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }

        return factory;
    }

    @Bean(name="newKafkaTemplate")
    public KafkaTemplate<String,Object> newKafkaTemplate(@Qualifier("newKafkaProducerFactory") ProducerFactory<String,Object> kafkaProducerFactory,
                                                ProducerListener<String,Object> kafkaProducerListener,
                                                ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<String,Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setDefaultTopic(kafkaProperties.getTemplate().getDefaultTopic());
        return kafkaTemplate;
    }

    /**
     * 初始化对kafka执行操作的对象
     * @return
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        KafkaAdmin admin = new KafkaAdmin(kafkaProperties.buildProducerProperties());
        return admin;
    }

    /**
     * 初始化操作连接
     * @return
     */
    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfigurationProperties());
    }
}
