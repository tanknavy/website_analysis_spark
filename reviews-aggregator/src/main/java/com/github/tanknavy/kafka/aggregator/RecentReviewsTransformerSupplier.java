package com.github.tanknavy.kafka.aggregator;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;

import com.github.tanknavy.avro.Review;

public class RecentReviewsTransformerSupplier implements TransformerSupplier<String, Review, KeyValue<String, Review>>{
	
	private Long timeToKeepAReview;
	private String stateStoreName;
	private RecentReviewsTransformer recentReviewsTransformer;
	
	
	public RecentReviewsTransformerSupplier(Long timeToKeepAReview,String stateStoreName) {
		super();
		this.timeToKeepAReview = timeToKeepAReview;
		this.stateStoreName = stateStoreName;
		this.recentReviewsTransformer = new RecentReviewsTransformer(timeToKeepAReview, stateStoreName);
	}


	@Override
	public Transformer<String, Review, KeyValue<String, Review>> get() {
		
		return recentReviewsTransformer;
	}

}
