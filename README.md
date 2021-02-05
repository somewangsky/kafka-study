# kafka学习

## 一、项目结构

1.kafka-clients依赖的是kafka-clients

```yml
<dependency>
     <groupId>org.apache.kafka</groupId>
     <artifactId>kafka-clients</artifactId>
     <version>2.0.0</version>
</dependency>
```

2.spring-kafka依赖的是spring-kafka

```yaml
<dependency>
     <groupId>org.springframework.kafka</groupId>
     <artifactId>spring-kafka</artifactId>
     <version>2.6.4</version>
</dependency>
```

## 二、kafka常见操作命令

1.创建主题

```shell
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic test --partitions 2 --replication-factor 1
```

2.查看所有主题

```shell
bin/kafka-topics.sh --zookeeper localhost:2181 --list
```

3.查看主题详情

```shell
bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic heima
```

4.修改主题

```shell
添加配置
bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic test --config flush.messages=1
删除配置
bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic test --delete-config flush.messages
```

5.删除主题

```shell
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic test
```

6.增加分区

```shell
bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic test --partitions 3
```

7.生产端发送消息

```shell
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
或者
cat test.txt | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

8.生产者消费消息

```shell
bin/kafka-console-consumer.sh --broker-list localhost:9092 --topic test
```

9.查看消费组

```shell
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

10.查看消费组详情

```shell
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group.demo
```

11.查看消费组当前的状态

```shell
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group.demo --state
```

12.查看消费组内成员信息

```shell
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group.demo --member
```

13.删除消费组，如果有消费者在使用会失败

```shell
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group group.demo
```

14.消费位移管理（重置消费位移，前提是没有消费者在消费）

```shell
 bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group group.demo --all-topics --reset-offsets --to-earliest --execute
```

