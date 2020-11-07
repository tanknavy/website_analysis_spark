package com.tanknavy.spark_www.streaming;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;


public class AccuBroa {
	
	
}

class WordBlacklistCounter{
	private static volatile org.apache.spark.util.LongAccumulator wordBlockList = null;
	
	public static LongAccumulator getInstance(JavaSparkContext jsc) {
		if (wordBlockList == null) {
			synchronized(WordBlacklistCounter.class) {
				if (wordBlockList == null) {
					wordBlockList = jsc.sc().longAccumulator("wordsInBlackCounter");
				}
			}
			
		}
		
		return wordBlockList;
	}
}

class WordBlacklist{
	private static volatile Broadcast<List<String>> instance = null;
	
	public static Broadcast<List<String>> getInstance(JavaSparkContext jsc){
		
		if(instance == null) {
			synchronized(WordBlacklist.class){
				if(instance == null) {
					List<String> list = Arrays.asList("a","b","c");
					instance = jsc.broadcast(list);
				}
			}
		}
		return instance;
	}
	
}