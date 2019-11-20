package com.github.tanknavy.kafka.producer.model;

import java.util.List;
import com.github.tanknavy.avro.Review;

/**
 *  网站api响应
 * @author admin
 *
 */

public class ReviewApiResponse {
	private Integer count;
	private String next;
	private String previos;
	private List<Review> reviewList;
	
	public ReviewApiResponse(Integer count, String next, String previos,
			List<Review> reviewList) {
		super();
		this.count = count;
		this.next = next;
		this.previos = previos;
		this.reviewList = reviewList;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
	public String getNext() {
		return next;
	}
	public void setNext(String next) {
		this.next = next;
	}
	public String getPrevios() {
		return previos;
	}
	public void setPrevios(String previos) {
		this.previos = previos;
	}
	public List<Review> getReviewList() {
		return reviewList;
	}
	public void setReviewList(List<Review> reviewList) {
		this.reviewList = reviewList;
	}
	
	
	
}
