package com.github.tanknavy.kafka.producer.reviews_fraud;

import com.typesafe.config.Config;

public class AppConfig {
    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String sourceTopicName;
    private final String validTopicName;
    private final String fraudTopicName;
    private final String applicationId;
	public AppConfig(Config config) { // 装载和使用配置文件，Config是不可以变map，值是json类型
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.sourceTopicName = config.getString("kafka.source.topic.name");
        this.validTopicName = config.getString("kafka.valid.topic.name");
        this.fraudTopicName = config.getString("kafka.fraud.topic.name");
        this.applicationId = config.getString("kafka.streams.application.id");
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}
	public String getSchemaRegistryUrl() {
		return schemaRegistryUrl;
	}
	public String getSourceTopicName() {
		return sourceTopicName;
	}
	public String getValidTopicName() {
		return validTopicName;
	}
	public String getFraudTopicName() {
		return fraudTopicName;
	}
	public String getApplicationId() {
		return applicationId;
	}
    
    
    
}
