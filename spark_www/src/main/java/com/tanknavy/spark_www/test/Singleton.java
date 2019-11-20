package com.tanknavy.spark_www.test;

public class Singleton {
	
	private static Singleton instance = null; // 引用自己的单例的实例对象
	private Singleton(){	
	}
	
	//public static synchronized Singleton getInstance(){ //为什么不这样？
	public static Singleton getInstance(){
		// 两部检查机制
		if (instance == null){
			// Only one thread can execute at a time. sync_object is a reference to an object
			// whose lock associates with the monitor. The code is said to be synchronized on the monitor object
			synchronized(Singleton.class) { //因为是静态方法，所以只能用该类的Class对象作为同步访问的监视器
				if (instance == null){
					instance = new Singleton();
				}
			}
		}
		return instance;

	}
}
