package com.github.tanknavy.kafka.producer.reviews_fraud;

import io.confluent.common.utils.Utils;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tanknavy.avro.Review;
import com.typesafe.config.ConfigFactory;


public class FraudMain {
	private Logger log = LoggerFactory.getLogger(FraudMain.class.getSimpleName());
	private AppConfig appConfig;
	
	public static void main(String[] args){
		FraudMain fraudMain = new FraudMain();
		fraudMain.start();
	}

	public FraudMain() { //构造函数
		appConfig = new AppConfig(ConfigFactory.load()); // 包含静态方法去装载配置属性，从默认位置读取
	}

	private void start() {
		// TODO Auto-generated method stub
		Properties config = getKafkaSteamsConfig();
		KafkaStreams streams = createTopology(config); // 从一个或者多个topic的输入进行连续计算,发送给一个或多个topic
		streams.cleanUp();
		streams.start();
		//streams.localThreadsMetadata();
		//new Thread(streams::close);
		//http://tutorials.jenkov.com/java/lambda-expressions.html
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
		// getRuntime获得和当前application相关的对象，注册停机hook, hook是初始化过但没有启动的Thread对象
		//Runtime.getRuntime().addShutdownHook(new Thread(()->{streams.close();}));
	}
	
	private Properties getKafkaSteamsConfig(){ // appConfig从配置文件读取信息,放入Properties
		Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 测试环境禁止cache, 生产环境不推荐
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        // exactly once processing
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());

        return config;

		}
	private KafkaStreams createTopology(Properties config) {
		// TODO Auto-generated method stub
		StreamsBuilder builder = new StreamsBuilder(); // kafka流
		KStream<Bytes, Review> reviews = builder.stream(appConfig.getSourceTopicName());
		
		// Branch(split)根据条件分支
		KStream<Bytes, Review>[] branches = reviews.branch(
				(k,review) -> isValidReview(review), // 第一个预知
				(k,review) -> true // 所有其它类
		) ;
		
		KStream<Bytes, Review> validReview = branches[0];
		KStream<Bytes, Review> fraudReview = branches[1];
		
		// peek每个看一下,不同分类送给不同的topic
		validReview.peek((k,review) -> log.info("Valid: " + review.getId())).to(appConfig.getValidTopicName()); // 物化stream to topic
		fraudReview.peek((k,review) -> log.info("Fraud: " + review.getId())).to(appConfig.getFraudTopicName()); // 物化stream to topic
		
		return new KafkaStreams(builder.build(), config);
	}

	private Boolean isValidReview(Review review) {
		// TODO Auto-generated method stub
		try{
			int hash = Utils.murmur2(review.toByteBuffer().array()); //review在avro中序列化为java.nio.ByteBuffer类型
			return (hash % 100) >=5; //简单的95%的是正确的
		}catch (IOException e){
			return false;
		}
	}
	
	
}
