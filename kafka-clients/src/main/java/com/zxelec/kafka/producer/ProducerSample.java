package com.zxelec.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * ProducerSample
 *
 * @author vimicro
 * @date 2021/1/28
 */
public class ProducerSample {

    private static final String SERVER_LIST = "10.150.10.113:9092,10.150.10.113:9093,10.150.10.113:9094";
    private static final String TOPIC_NAME = "part3-topic";
    private static final String TRANSACTION_ID = "transaction_id";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 同步发送
        // syncSend();

        // 异步发送
        // asyncSend();

        // 异步回调发送
        // asyncSendCallback();

        // 自定义分区器
        // syncSendByCustomerPartitioner();

        // 自定义拦截器
        // syncSendByCustomerInterceptor();

        // 带事务的提交
        sendByTransaction();
    }

    private static Producer<String, String> getProducer() {
        Properties properties = new Properties();
        /**
         * 该属性指定brokers的地址清单，格式为host:port。清单里不需要包含所有的broker地址,
         * 生产者会从指定的broker里面查找到其它broker的信息。建议至少提供两个broker的信息，
         * 因为一旦其中一个宕机，生产者仍然能够连接到集群上。
         */
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_LIST);
        /**
         * 三个取值："0"、"1"、"all"
         */
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        /**
         * 生产者会使用这个对象序列化key，kafka默认提供了StringSerializer和IntegerSerializer、
         * ByteArraySerializer。当然也可以自定义序列化器。
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    /**
     * 自定义分区分配
     *
     * @return
     */
    private static Producer<String, String> getProducerByCustomerPartitioner() {
        Properties properties = new Properties();
        /**
         * 该属性指定brokers的地址清单，格式为host:port。清单里不需要包含所有的broker地址,
         * 生产者会从指定的broker里面查找到其它broker的信息。建议至少提供两个broker的信息，
         * 因为一旦其中一个宕机，生产者仍然能够连接到集群上。
         */
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_LIST);
        /**
         * 三个取值："0"、"1"、"all"
         */
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        /**
         * 自定义分区器
         */
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomerPartitioner.class.getName());
        /**
         * 生产者会使用这个对象序列化key，kafka默认提供了StringSerializer和IntegerSerializer、
         * ByteArraySerializer。当然也可以自定义序列化器。
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    /**
     * 自定义生产者的前置拦截器
     *
     * @return
     */
    private static Producer<String, String> getProducerWithInterceptor() {
        Properties properties = new Properties();
        /**
         * 该属性指定brokers的地址清单，格式为host:port。清单里不需要包含所有的broker地址,
         * 生产者会从指定的broker里面查找到其它broker的信息。建议至少提供两个broker的信息，
         * 因为一旦其中一个宕机，生产者仍然能够连接到集群上。
         */
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_LIST);
        /**
         * 三个取值："0"、"1"、"all"
         */
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        /**
         * 自定义生产者拦截器
         */
        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CustomerProducerInterceptor.class.getName());

        /**
         * 生产者会使用这个对象序列化key，kafka默认提供了StringSerializer和IntegerSerializer、
         * ByteArraySerializer。当然也可以自定义序列化器。
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    /**
     * 生产者添加事务
     *
     * @return
     */
    private static Producer<String, String> getProducerWithTransaction() {
        Properties properties = new Properties();
        /**
         * 该属性指定brokers的地址清单，格式为host:port。清单里不需要包含所有的broker地址,
         * 生产者会从指定的broker里面查找到其它broker的信息。建议至少提供两个broker的信息，
         * 因为一旦其中一个宕机，生产者仍然能够连接到集群上。
         */
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_LIST);
        /**
         * 三个取值："0"、"1"、"all"
         */
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "1");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        /**
         * 添加事务
         */
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTION_ID);
        /**
         * 生产者会使用这个对象序列化key，kafka默认提供了StringSerializer和IntegerSerializer、
         * ByteArraySerializer。当然也可以自定义序列化器。
         */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    /**
     * 异步阻塞发送数据（约等于同步）
     */
    public static void syncSend() throws ExecutionException, InterruptedException {
        Producer<String, String> producer = getProducer();
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + i, "value:" + i);
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println("key:" + i + " " + "value:" + i + " send to topic is:" + recordMetadata.topic() + " " + "partition is:" +
                    recordMetadata.partition() + " " + "offset is:" + recordMetadata.offset());
        }
        producer.close();
    }

    /**
     * 异步发送数据
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void asyncSend() throws ExecutionException, InterruptedException {
        Producer<String, String> producer = getProducer();
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + i, "value:" + i);
            // 带时间戳的消息
            ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(TOPIC_NAME, 0, System.currentTimeMillis() - 10 * 1000, "key:" + i, "value:" + i);

            Future<RecordMetadata> recordMetadataFuture = producer.send(record1);
            Future<RecordMetadata> recordMetadataFuture2 = producer.send(record2);
        }
        producer.close();
    }

    /**
     * 异步回调发送
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void asyncSendCallback() throws ExecutionException, InterruptedException {
        Producer<String, String> producer = getProducer();
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + i, "value:" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        System.out.println("Message send to topic is:" + metadata.topic() + " " + "partition is:" +
                                metadata.partition() + " " + "offset is:" + metadata.offset());
                    }
                }
            });
        }
        producer.close();
    }

    /**
     * 自定义分区器
     */
    public static void syncSendByCustomerPartitioner() throws ExecutionException, InterruptedException {
        Producer<String, String> producer = getProducerByCustomerPartitioner();
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + i, "value:" + i);
            Future<RecordMetadata> recordMetadataFuture = producer.send(record);
            RecordMetadata recordMetadata = recordMetadataFuture.get();
            System.out.println("key:" + i + " " + "value:" + i + " send to topic is:" + recordMetadata.topic() + " " + "partition is:" +
                    recordMetadata.partition() + " " + "offset is:" + recordMetadata.offset());
        }
        producer.close();
    }

    /**
     * 自定义生产者过滤器
     */
    public static void syncSendByCustomerInterceptor() {
        Producer<String, String> producer = getProducerWithInterceptor();
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + i, "value:" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("Message send to topic is:" + metadata.topic() + " " + "partition is:" +
                            metadata.partition() + " " + "offset is:" + metadata.offset());
                    throw new RuntimeException();
                }
            });
        }
        producer.close();
    }

    /**
     * 带事务的提交
     */
    public static void sendByTransaction() {
        Producer<String, String> producer = getProducerWithTransaction();
        // 初始化事务
        producer.initTransactions();
        // 开启事务
        producer.beginTransaction();

        try {
            // 处理业务逻辑
            ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + 1, "value:" + 1);
            producer.send(record1);

            ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + 2, "value:" + 2);
            producer.send(record2);

            int i = 1 / 0;

            ProducerRecord<String, String> record3 = new ProducerRecord<String, String>(TOPIC_NAME, "key:" + 3, "value:" + 3);
            producer.send(record3);

            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            // 回滚事务
            producer.abortTransaction();
        }

        producer.close();
    }
}
