package com.zxelec.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * AdminController
 *
 * @author vimicro
 * @date 2021/2/5
 */
@RestController
@Slf4j
public class AdminController {

    @Autowired
    private AdminClient adminClient;

    /**
     * 创建topic
     *
     * @param topicName
     * @param partitionNum
     * @param replicaNum
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping("/create/topic")
    public String createTopic(
            @RequestParam(value = "topicName", required = true) String topicName,
            @RequestParam(value = "partitionNum", required = true) Integer partitionNum,
            @RequestParam(value = "replicaNum", required = true) Short replicaNum) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, partitionNum, replicaNum);
        CreateTopicsResult result = adminClient.createTopics(Arrays.asList(newTopic));
        result.all().get();
        return "success";
    }

    /**
     * 增加topicPartition的个数
     *
     * @param topicName
     * @param partitionNum
     * @return
     */
    @GetMapping("/add/topicPartitions")
    public String addTopicPartitions(
            @RequestParam(value = "topicName") String topicName,
            @RequestParam(value = "partitionNum") Integer partitionNum) throws ExecutionException, InterruptedException {
        NewPartitions newPartitions = NewPartitions.increaseTo(partitionNum);
        Map<String, NewPartitions> newPartitionsMap = new HashMap<>(10);
        newPartitionsMap.put(topicName, newPartitions);
        CreatePartitionsResult result = adminClient.createPartitions(newPartitionsMap);
        result.all().get();

        return "success";
    }

    /**
     * 修改topic的配置信息
     *
     * @param topicName
     * @param configName
     * @param configValue
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping("/alter/topicConfig")
    public String alterTopicConfig(
            @RequestParam(value = "topicName") String topicName,
            @RequestParam(value = "configName") String configName,
            @RequestParam(value = "configValue") String configValue) throws ExecutionException, InterruptedException {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        // ConfigEntry configEntry = new ConfigEntry("cleanup.policy", "compact");
        ConfigEntry configEntry = new ConfigEntry(configName, configValue);
        Config config = new Config(Collections.singleton(configEntry));
        Map<ConfigResource, Config> configs = new HashMap<>();
        configs.put(configResource, config);

        AlterConfigsResult result = adminClient.alterConfigs(configs);
        result.all().get();

        return "success";
    }

    /**
     * 获取所有topic信息
     *
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping("/list/topics")
    public Set<String> listTopics() throws ExecutionException, InterruptedException {
        ListTopicsOptions options = new ListTopicsOptions();
        // 显示kafka内部的topic
        options.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
        Set<String> names = listTopicsResult.names().get();

        return names;
    }

    /**
     * 删除topics
     *
     * @param topics
     * @return
     */
    @GetMapping("/delete/topics")
    public String deleteTopics(
            @RequestParam(value = "topics") List<String> topics) {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);

        return "success";
    }

    /**
     * 查看topic详情
     *
     * @param topicNames
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping("/describe/topics")
    public Map<String, TopicDescription> describeTopics(
            @RequestParam(value = "topicNames") List<String> topicNames) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicNames);
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        topicDescriptionMap.forEach((k, v) -> {
            if (log.isInfoEnabled()) {
                log.info("topicName:{}----------TopicDescription:{}", k, v);
            }
        });
        return topicDescriptionMap;
    }

    /**
     * 查看topic的配置信息
     *
     * @param topicName
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @GetMapping("/describe/topicConfig")
    public Map<ConfigResource, Config> describeTopicConfig(
            @RequestParam(value = "topicName") String topicName) throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        DescribeConfigsResult result = adminClient.describeConfigs(Collections.singleton(resource));
        Map<ConfigResource, Config> descriptionMap = result.all().get();
        descriptionMap.forEach((k, v) -> {
            if (log.isInfoEnabled()) {
                log.info("topicName:{}----------Config:{}", k, v);
            }
        });
        return descriptionMap;
    }
}
