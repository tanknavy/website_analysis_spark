package com.github.tanknavy.kafka.producer.runnable;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tanknavy.avro.Review;
import com.github.tanknavy.kafka.producer.AppConfig;
import com.github.tanknavy.kafka.producer.client.AlexRESTClient;

public class ReviewsFetcherThread implements Runnable{
	
	
	private Logger log = LoggerFactory.getLogger(ReviewsFetcherThread.class.getSimpleName());
	
    private final AppConfig appConfig;
    private final ArrayBlockingQueue<Review> reviewsQueue;
    private final CountDownLatch latch;
    private AlexRESTClient alexRESTClient;
    
    // 构造函数
	public ReviewsFetcherThread(AppConfig appConfig,
			ArrayBlockingQueue<Review> reviewsQueue, CountDownLatch latch) {
		super();
		this.appConfig = appConfig;
		this.reviewsQueue = reviewsQueue;
		this.latch = latch;
		alexRESTClient = new AlexRESTClient(appConfig.getCourseId(), appConfig.getPageSize());
	}


	// 实现Runnable接口的run方法
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			Boolean keepOnRunning = true;
			while(keepOnRunning){
				List<Review> reviews;
				try{
					reviews = alexRESTClient.getNextReviews(); 
					log.info("Fetched " + reviews.size() + " reviews");
					if(reviews.size() == 0){
						keepOnRunning = false; //无料可读停止
					} else{
						// 有料可读，进入线程安全队列ArrayBlockingQueue
						log.info("Queue size : " + reviewsQueue.size());
						for(Review review:reviews){
							reviewsQueue.put(review);
						}
					}
				}catch(HttpException e){ // 读取错误的花
					e.printStackTrace();
					Thread.sleep(500);
				}finally{
					Thread.sleep(50);
				}
			}
		}catch(InterruptedException e){ //终端错误
			log.warn("REST client interruppted");
		}finally{
			this.close();
		}
	}


	private void close() {
		// TODO Auto-generated method stub
		log.info("Closing");
		alexRESTClient.close();
		latch.countDown();// 每次调用，计数减一，
		log.info("Closed");
	}

}
