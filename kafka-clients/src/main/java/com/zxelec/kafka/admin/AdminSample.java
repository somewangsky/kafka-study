package com.zxelec.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * AdminSample
 *
 * @author vimicro
 * @date 2021/2/4
 */
public class AdminSample {

    private static final String BROKER_LIST = "10.150.10.113:9092,10.150.10.113:9093,10.150.10.113:9094";
    private static final String TOPIC_NAME = "test-topic";
    private static final String DELETE_TOPIC_NAME = "WifiFenceDevices";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 创建topic
        // createTopic();

        // 修改分区的个数
        // addTopicPartitions();

        // 修改topic的配置
        // alterTopicConfig();

        // 查看topic的配置信息
        // describeTopicConfig();

        // 查看topic的详细信息
        // describeTopic();

        // 查看所有的topic
        // listTopics();

        // 批量删除
        deleteTopics();
    }

    public static AdminClient client() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    /**
     * 创建主题
     */
    public static void createTopic() {
        AdminClient client = client();

        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0, Arrays.asList(0));
        replicasAssignments.put(1, Arrays.asList(0));
        // NewTopic newTopic = new NewTopic(TOPIC_NAME, replicasAssignments);
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 2, (short) 1);
        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        client.close();
    }

    /**
     * 添加主题的partition
     * tip:修改分区只能增加不能修改
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void addTopicPartitions() throws ExecutionException, InterruptedException {
        AdminClient client = client();

        NewPartitions newPartitions = NewPartitions.increaseTo(3);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>();
        newPartitionsMap.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult result = client.createPartitions(newPartitionsMap);
        result.all().get();

        client.close();
    }

    /**
     * 修改topic的配置信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void alterTopicConfig() throws ExecutionException, InterruptedException {
        AdminClient client = client();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        ConfigEntry configEntry = new ConfigEntry("cleanup.policy", "compact");
        Config config = new Config(Collections.singleton(configEntry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(configResource, config);

        AlterConfigsResult result = client.alterConfigs(configs);
        result.all().get();

        client.close();
    }

    /**
     * 查看topic的配置信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void describeTopicConfig() throws ExecutionException, InterruptedException {
        AdminClient client = client();

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));

        // Config config = result.all().get().get(resource);
        // System.out.println(config);

        Map<ConfigResource, Config> descriptionMap = result.all().get();
        descriptionMap.forEach((k, v) -> {
            System.out.println("topicName:" + k + "       " + "Config:" + v);
        });

        client.close();
    }

    /**
     * 查看topic的详细信息
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void describeTopic() throws ExecutionException, InterruptedException {
        AdminClient client = client();
        DescribeTopicsResult describeTopicsResult = client.describeTopics(Collections.singleton(TOPIC_NAME));

        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        topicDescriptionMap.forEach((k, v) -> {
            System.out.println("topicName:" + k + "       " + "TopicDescription:" + v);
        });

        client.close();
    }

    /**
     * 显示所有的topic主题
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void listTopics() throws ExecutionException, InterruptedException {
        AdminClient client = client();

        ListTopicsOptions options = new ListTopicsOptions();
        // 显示kafka内部的topic
        options.listInternal(true);
        ListTopicsResult listTopicsResult = client.listTopics(options);

        Set<String> names = listTopicsResult.names().get();
        names.forEach(System.out::println);

        client.close();
    }

    /**
     * 批量删除topic
     */
    public static void deleteTopics() throws ExecutionException, InterruptedException {
        AdminClient client = client();
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Arrays.asList(DELETE_TOPIC_NAME));
        System.out.println("DeleteTopicsResult:" + deleteTopicsResult.all().get());
    }
}