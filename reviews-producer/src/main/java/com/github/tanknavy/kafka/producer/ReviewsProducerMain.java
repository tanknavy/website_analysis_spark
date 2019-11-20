package com.github.tanknavy.kafka.producer;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.tanknavy.avro.Review;
import com.github.tanknavy.kafka.producer.runnable.ReviewsAvroProducerThread;
import com.github.tanknavy.kafka.producer.runnable.ReviewsFetcherThread;
import com.typesafe.config.ConfigFactory;

/**
 * https://github.com/simplesteph/medium-blog-kafka-udemy
 * @author admin
 *
 */

public class ReviewsProducerMain {
	
	
	private Logger log = LoggerFactory.getLogger(ReviewsProducerMain.class.getSimpleName());
	// 线程安全队列在满的时候
	private ExecutorService executor; // 管理结束和跟踪异步任务
	private CountDownLatch latch; //  同步辅助类，容许一个或者多个线程一直等待直到其它线程执行完毕才开始执行
	private ReviewsFetcherThread myRESTClient;
	private ReviewsAvroProducerThread reviewsProducer;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReviewsProducerMain app = new ReviewsProducerMain();
		app.start();
	}

	public ReviewsProducerMain() {
		super();
		AppConfig appConfig = new AppConfig(ConfigFactory.load()); // 包含创建config的静态方法
		latch = new CountDownLatch(2); // 要被等待执行完成的线程个数
		executor = Executors.newFixedThreadPool(2); // Executors执行提交的runnable任务
		ArrayBlockingQueue<Review> reviewsQueue = new ArrayBlockingQueue<>(appConfig.getQueueCapacity());
		myRESTClient = new ReviewsFetcherThread(appConfig, reviewsQueue, latch);
		reviewsProducer = new ReviewsAvroProducerThread(appConfig, reviewsQueue, latch);
		
	}

	private void start() {
		// TODO Auto-generated method stub
		// java 多线程和lambda函数 
		// 怎么没有override run方法？ 查看Thread()这个无参构造方法调用init进行初始化，这里是重写构造方法？
		//https://www.geeksforgeeks.org/lambda-expressions-java-8/
		//http://tutorials.jenkov.com/java-concurrency/creating-and-starting-threads.html
		Runtime.getRuntime().addShutdownHook(new Thread(() ->{ // runtime返回当前程序的运行对象，
			if(!executor.isShutdown()){ // 管理线程termination
				log.info("shutdown requested");
				shutdown(); // 如何停止其它相关线程
			}
		})); 
		
		log.info("Application started");
		executor.submit(myRESTClient); // 2个线程池提交任务
		executor.submit(reviewsProducer); // 2个线程池提交任务
		
		try{
			log.info("latch await");
			latch.await(); //主程序执行到await函数会阻塞等待线程执行，直到计数为0
			log.info("Threads completed"); //两个线程都执行完成，主线程继续
		} catch (InterruptedException e){
			e.printStackTrace();
		} finally {
			shutdown();
			log.info("Applicatio closed successfully");
		}
	}

	private void shutdown() {
		// TODO Auto-generated method stub
		if (!executor.isShutdown()){
			log.info("shutting down");
			executor.shutdown(); // 准备shutdown
			try{
				if(!executor.awaitTermination(2000, TimeUnit.MILLISECONDS)){ //如果指定时间内没有terminate
					log.warn("Executor did not terminate in the specified time.");
					List<Runnable> droppedTasks = executor.shutdownNow();
					log.warn("Executor was abruptly shut down. " + droppedTasks.size() + " tasks will not be executed.");
				}
			} catch(InterruptedException e){
				e.printStackTrace();
			}
		}
	}

}
