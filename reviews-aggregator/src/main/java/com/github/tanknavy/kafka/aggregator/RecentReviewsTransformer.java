package com.github.tanknavy.kafka.aggregator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import com.github.tanknavy.avro.Review;

//The Transformer interface is for stateful mapping
//Use TransformerSupplier to provide new instances of Transformer to Kafka Stream's runtime. 
public class RecentReviewsTransformer implements Transformer<String, Review, KeyValue<String, Review>>{
	private Logger log = LoggerFactory.getLogger(RecentReviewsTransformer.class.getSimpleName());
	private ProcessorContext context;
	private KeyValueStore<Long, Review> reviewStore; // 支持put/get/delete and range quries
	private Long timeToKeepReview;
	private String stateStoreName;
	private Long minTimestampInStore = -1L;
	
	public RecentReviewsTransformer(Long timeToKeepReview, String stateStoreName) {
		super();
		this.timeToKeepReview = timeToKeepReview;
		this.stateStoreName = stateStoreName;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(ProcessorContext context) { //Processor context interface
		// 保持processor context 本地
		this.context = context;
		
		//调用punctuate()方法每10分钟去清理旧的数据,Schedules a periodic operation for processors
		this.context.schedule(Duration.ofMinutes(10), PunctuationType.STREAM_TIME,this::punctuate);
		
	    // A key-value store that supports put/get/delete and range queries
		reviewStore = (KeyValueStore<Long, Review>) this.context.getStateStore(stateStoreName);
		
	}
	
	//Transform the record with the given key and value.
	@Override
	public KeyValue<String, Review> transform(String courseId, Review review) { //装换成k/v送给下游
		// TODO Auto-generated method stub
		Long reviewId = review.getId();
		Long now = System.currentTimeMillis(); // 当前时间点
		// 如果没有找到，并且没有过期
		if(reviewStore.get(reviewId) == null && !isReviewExpired(review,now,timeToKeepReview)){
			reviewStore.put(review.getId(), review);
			updateMinTimestamp(review); // 每放入一个更新一次review上最小时间戳
			return KeyValue.pair(courseId, review);
		}else{
		return null;
		}
	}
	private boolean isReviewExpired(Review review, Long now,
			Long timeToKeepReview2) {
		// TODO Auto-generated method stub
		return review.getCreated().getMillis() + timeToKeepReview2   < now;
	}
	private void updateMinTimestamp(Review review) {
		// 已经存储的最小时间戳 和 对象的创建时间
		minTimestampInStore = Math.min(minTimestampInStore, review.getCreated().getMillis());
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		// do nothing
	}
	
	// 每个punctuate过期旧的reviews
	public void punctuate(long currentTime){ //强调？
		// 如果最小时间+保留时间 小于  当前时间， 并且store有料
		if(minTimestampInStore + timeToKeepReview < currentTime  && reviewStore.approximateNumEntries() >0){
			log.info("let's expire data!");
			minTimestampInStore = System.currentTimeMillis();
			KeyValueIterator<Long, Review> it = reviewStore.all();
			List<Long> keys2Remove = new ArrayList<>();
			while (it.hasNext()){
				KeyValue<Long, Review> next = it.next();
				Review review = next.value;
				Long courseId = review.getCourse().getId();
				if(isReviewExpired(review, currentTime, timeToKeepReview)){ //如果时间过期
					Long reviewId = next.key;
					keys2Remove.add(reviewId);
					// 反向review事件从average中移除data
					Review reverseReview = reverseReview(review); // 将打分变成负数，就是扣除
				}else{
					updateMinTimestamp(review);
				}
			}for(Long key: keys2Remove){
				reviewStore.delete(key);
			}
		}
	}
	private Review reverseReview(Review review) {
		// TODO Auto-generated method stub
		// 比如设置3.5为 -3.5,就是减掉
		return Review.newBuilder(review).setRating("-" + review.getRating()).build();
	}
	
}
