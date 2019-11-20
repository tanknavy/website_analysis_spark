package com.tanknavy.spark_www.test;

import com.tanknavy.spark_www.conf.ConfigurationManager;

public class ConfigurationManagerTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String test1 = ConfigurationManager.getProperty("key1");
		String test2 = ConfigurationManager.getProperty("key2");
		
		System.out.println(test1 + ";" + test2);
	}

}
