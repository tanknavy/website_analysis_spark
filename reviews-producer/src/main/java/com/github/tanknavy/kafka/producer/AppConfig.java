package com.github.tanknavy.kafka.producer;

import com.typesafe.config.Config;
/**
 * kafka配置信息
 * 
 *
 */

public class AppConfig {

    private final String bootstrapServers; // kafka集群
    private final String schemaRegistryUrl; // avro schem注册地址
    private final String topicName;
    private final Integer queueCapacity; // 队列大小
    private final Integer producerFrequencyMs; 
    private final Integer pageSize;
    private final String courseId;

    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.topicName = config.getString("kafka.topic.name");
        this.queueCapacity = config.getInt("app.queue.capacity");
        this.producerFrequencyMs = config.getInt("app.producer.frequency.ms");
        this.pageSize = config.getInt("app.udemy.page.size");
        this.courseId = Long.toString(config.getLong("app.course.id"));
    }


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getTopicName() {
        return topicName;
    }

    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    public Integer getProducerFrequencyMs() {
        return producerFrequencyMs;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public String getCourseId() {
        return courseId;
    }


}