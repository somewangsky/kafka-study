package com.zxelec.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

/**
 * ConsumerSample
 *
 * @author vimicro
 * @date 2021/1/28
 */
public class ConsumerSample {
    private static final String BROKER_LIST = "10.150.10.113:9092,10.150.10.113:9093,10.150.10.113:9094";
    private static final String GROUP_ID = "wang";
    private static final String TOPIC_NAME = "part3-topic";

    public static void main(String[] args) {
        // 自动提交offset的消费
        // autoOffsetConsumer();

        // 手动同步提交offset消费
        // syncOffsetConsumer();

        // 异步提交offset消费
        // AsyncOffsetConsumer();

        // 指定offset消费
        // seekOffsetConsumer();

        // 增加判断是否分配到分区，指定offset消费
        // seekOffsetAssignmentConsumer();

        // 将offset设置到末尾进行消费
        // seekOffsetEndConsumer();

        // 在均衡监听器
        // commitSyncInRebalance();

        // 自定义消费者拦截器
        customerInterceptorConsumer();
    }

    /**
     * offset自动提交
     *
     * @return
     */
    private static KafkaConsumer getAutoOffsetConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         * 消费者隶属于的消费者，默认为空，如果设置为空，则会抛出异常，这个参数要设置成具有一定业务含义的名称
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        /**
         * 开启offset自动commit，会导致重复消费或者数据丢失的情况
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        /**
         * 在poll方法调用后的时间间隔提交一次位移
         */
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * 指定kafkaConsumer对应的客户端ID，默认为空，如果不设置kafkaConsumer会自动生成一个非空字符串
         */
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    /**
     * offset手动同步提交
     *
     * @return
     */
    private static KafkaConsumer getSyncOffsetConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         * 消费者隶属于的消费者，默认为空，如果设置为空，则会抛出异常，这个参数要设置成具有一定业务含义的名称
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "somewang001");
        /**
         * 开启offset手动commit
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        /**
         * 在poll方法调用后的时间间隔提交一次位移
         */
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * 指定kafkaConsumer对应的客户端ID，默认为空，如果不设置kafkaConsumer会自动生成一个非空字符串
         */
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    /**
     * offset自动提交
     *
     * @return
     */
    private static KafkaConsumer getInterceptorConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         * 消费者隶属于的消费者，默认为空，如果设置为空，则会抛出异常，这个参数要设置成具有一定业务含义的名称
         */
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        /**
         * 开启offset自动commit，会导致重复消费或者数据丢失的情况
         */
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        /**
         * 在poll方法调用后的时间间隔提交一次位移
         */
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /**
         * 指定kafkaConsumer对应的客户端ID，默认为空，如果不设置kafkaConsumer会自动生成一个非空字符串
         */
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer.client.id.demo");

        /**
         * 添加指定拦截器
         */
        properties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomerConsumerInterceptor.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        return consumer;
    }

    /**
     * 自动提交offset
     */
    public static void autoOffsetConsumer() {
        KafkaConsumer consumer = getAutoOffsetConsumer();
        // 获取主题所有分区的消息
        // consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 利用正则表达式匹配多个主题，而且订阅之后如果又有匹配的新主题，那么这个消费组会立即对其进行消费
        // consumer.subscribe(compile("part3*"));

        // 指定分区进行消费
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC_NAME, 0)));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 手动同步提交offset
     * <p>
     * 手动提交有一个缺点，那就是当发起提交调用时应用会阻塞。当然我们可以减少手动提交的频率
     * 但这个会增加消息重复的概率（和自动提交一样）。另一个解决办法是，使用异步提交的api
     */
    public static void syncOffsetConsumer() {
        KafkaConsumer consumer = getSyncOffsetConsumer();
        // 指定分区进行消费
        TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
        consumer.assign(Collections.singletonList(topicPartition));
        long lastConsumedOffset = -1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                continue;
            }
            List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
            lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            // 同步提交消费位移
            consumer.commitSync();
            System.out.println("consumed offset is " + lastConsumedOffset);
            OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
            System.out.println("committed offset is " + offsetAndMetadata.offset());
            long position = consumer.position(topicPartition);
            System.out.println("the offset of the next record is " + position);
        }
    }

    /**
     * 异步提交offset
     * <p>
     * 异步提交也有一个缺点，那就是如果服务器返回提交失败，异步提交不会进行重试。相比较起来，同步提交会
     * 进行重试直到成功或者最后抛出异常给应用。异步提交没有实现重试是因为，如果同时存在多个异步提交，进行
     * 重试可能会导致位移覆盖。举个例子，假如我们发起了一个异步提交commitA，此时的提交位移为2000，随后
     * 又发起了一个异步提交commitB且位移为3000；commitA提交失败但commitB提交成功，此时commitA进行重试
     * 并成功的话，会将实际已经提交的位移从3000回滚到2000，导致消息重复消费
     */
    public static void AsyncOffsetConsumer() {
        AtomicBoolean running = new AtomicBoolean(true);
        KafkaConsumer consumer = getSyncOffsetConsumer();
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    //do some logical processing.
                }
                // 异步回调
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           Exception exception) {
                        if (exception == null) {

                            System.out.println(offsets);
                        } else {
                            log.error("fail to commit offsets {}", offsets, exception);
                        }
                    }
                });
            }
        } finally {
            consumer.close();
        }
        try {
            while (running.get()) {
                consumer.commitAsync();
            }
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 指定offset消费
     * <p>
     * 这里有一个问题就是timeout参数如果设置不合适，会获取不到partition
     */
    public static void seekOffsetConsumer() {
        KafkaConsumer consumer = getAutoOffsetConsumer();
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        // timeout参数设置多少合适？太短会使分区分配失败，太长又有可能造成一些不必要的等待
        consumer.poll(Duration.ofMillis(10000));
        // 获取消费者所分配到的分区
        Set<TopicPartition> assignment = consumer.assignment();
        System.out.println(assignment);
        for (TopicPartition tp : assignment) {
            // 参数partition表示分区，offset表示指定从分区的哪个位置开始消费
            consumer.seek(tp, 10);
        }
//        consumer.seek(new TopicPartition(topic,0),10);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 增加判断是否分配到分区
     */
    public static void seekOffsetAssignmentConsumer() {
        KafkaConsumer consumer = getAutoOffsetConsumer();
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        long start = System.currentTimeMillis();
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println(assignment);
        for (TopicPartition tp : assignment) {
            consumer.seek(tp, 10);
        }
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 将offset设置到末尾进行消费
     */
    public static void seekOffsetEndConsumer() {
        KafkaConsumer consumer = getAutoOffsetConsumer();
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        // 指定从分区末尾开始消费
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for (TopicPartition tp : assignment) {
            // 从末尾开始消费
            // consumer.seek(tp, offsets.get(tp));

            // 从头开始消费
            consumer.seek(tp, offsets.get(tp) + 1);
        }
        System.out.println(assignment);
        System.out.println(offsets);

        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));
            //consume the record.
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 再均衡是指分区的所属从一个消费者转移到另外一个消费的行为，它为消费组具备了高可用性和伸缩性提供了保障，
     * 使得我们既方便又安全地删除消费组内的消费者或者往消费者内添加消费者。不过在均衡发生期间，消费者是无法拉取
     * 消息的
     */
    public static void commitSyncInRebalance() {
        AtomicBoolean isRunning = new AtomicBoolean(true);
        KafkaConsumer consumer = getAutoOffsetConsumer();
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // 尽量避免重复消费
                consumer.commitSync(currentOffsets);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //do nothing.
            }
        });
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    // 异步提交消费位移，在发生再均衡动作之前可以通过再均衡监听器的onPartitionsRevoked回调执行commitSync方法同步提交位移。
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffsets, null);
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 自定义kafka消费端的过滤器
     */
    public static void customerInterceptorConsumer() {
        KafkaConsumer consumer = getInterceptorConsumer();
        // 获取主题所有分区的消息
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d,offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
