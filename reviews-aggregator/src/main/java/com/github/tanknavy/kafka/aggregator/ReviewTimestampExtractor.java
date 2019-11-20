package com.github.tanknavy.kafka.aggregator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.github.tanknavy.avro.Review;

public class ReviewTimestampExtractor implements TimestampExtractor{

	@Override
	public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
		// 抽取正确的时间戳
		long timestamp = -1;
		final Review review = (Review) record.value();
		if (review != null){
			timestamp = review.getCreated().getMillis(); //创建时间距离1970的毫秒数
		}
		if (timestamp < 0){ // 非法时间戳，估计一个时间
			if (previousTimestamp >= 0){
				return previousTimestamp;
			}else{
				return System.currentTimeMillis();
			}
		}else{
			return  timestamp;
		}

	}

}
