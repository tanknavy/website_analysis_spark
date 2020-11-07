package com.tanknavy.spark_www.test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


// kafka 数据生产者
public class MockRealTimeData extends Thread{

	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"California", "Hubei", "Texas", "Washington", "Nevada"};
	private static final Map<String, String[]> provinceMap = new HashMap<String, String[]>();
	
	private Producer<Integer, String> producer;
	
	public MockRealTimeData(){ //构造函数
		provinceMap.put("California", new String[]{"LosAngeles","SanDiego","SanFrancisco"});
		provinceMap.put("Hubei", new String[]{"Wuhan","HuangGang"});
		provinceMap.put("Texas", new String[]{"Houston","Dallas"});
		provinceMap.put("Washington", new String[]{"Seattle","Portland"});
		provinceMap.put("Nevada", new String[]{"LasVegas","Reno"}); // 注意字符串带有空格
		
		producer = new Producer<Integer,String>(createProducerConfig());
	}
	
	private ProducerConfig createProducerConfig() {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder"); //序列化
		props.put("metadata.broker.list", "localhost:9092"); // kafaka主机
		return new ProducerConfig(props);
		
	}
	
	public void run(){
		while(true){
			String province = provinces[random.nextInt(provinces.length)];
			String city = provinceMap.get(province)[random.nextInt(2)];
			
			// 生产实时日志(timestamp,province,city,userid,adid) 1575700483925 California Los Angeles 20 5
			String log = new Date().getTime() + " " + province + " " + city + " " + random.nextInt(20) + " " +  random.nextInt(10);
			producer.send(new KeyedMessage<Integer, String>("AdRealTimeLog", log)); //发送消息到kafka
			
			try{
				Thread.sleep(100); //休眠100毫秒
				//System.out.println(log);
			} catch(InterruptedException e){
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData producer = new MockRealTimeData();
		producer.start();
	}

}
