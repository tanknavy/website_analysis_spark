package com.github.tanknavy.kafka.producer.runnable;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tanknavy.avro.Review;
import com.github.tanknavy.kafka.producer.AppConfig;

public class ReviewsAvroProducerThread  implements Runnable{
	private Logger log = LoggerFactory.getLogger(ReviewsAvroProducerThread.class.getSimpleName());
	
	private final AppConfig appConfig;
	private final ArrayBlockingQueue<Review> reviewQueue;
	private final CountDownLatch latch;
	private final KafkaProducer<Long, Review> kafkaProducer;
	private final String targetTopic;
	
	public ReviewsAvroProducerThread(AppConfig appConfig, ArrayBlockingQueue<Review> reviewsQueue,CountDownLatch latch){
		this.appConfig = appConfig;
		this.reviewQueue = reviewsQueue;
		this.latch = latch; 
		this.kafkaProducer = createKafkaProducer(appConfig);
		this.targetTopic = appConfig.getTopicName();
	}

	public KafkaProducer<Long, Review> createKafkaProducer(AppConfig appConfig2) {
		// TODO Auto-generated method stub
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,appConfig.getBootstrapServers());
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
		properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
		properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName()); //k/v的序列化
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());
		return new KafkaProducer<>(properties);

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int reviewCount = 0;
		try{
			while (latch.getCount() > 1 || reviewQueue.size() > 0){ // 闭锁大约0这是调用次多线程的主线程还在阻塞等待中
				Review review = reviewQueue.poll(); // 队列头取出一个
				if(review == null){
					Thread.sleep(200); // 无料等待
				} else{
					reviewCount += 1;
					log.info("Sending review " + reviewCount + ": " + review);
					//kafkaProducer.send(new ProducerRecord<Long, Review>(targetTopic, review));
					kafkaProducer.send(new ProducerRecord<>(targetTopic, review)); //可以不写类型，自己匹配
					Thread.sleep(appConfig.getProducerFrequencyMs()); // 有料等待
				}
			}
		} catch(InterruptedException e){
			log.warn("Avro Producer intterrupted");
		}
		finally{
			close();
		}
	}
	
	public void close(){
		log.info("Closing Producer");
		kafkaProducer.close(); //关闭发送
		latch.countDown(); //闭锁减一，表示自己已经完成，主调用的线程可以继续执行
	}
	
	
	
}
