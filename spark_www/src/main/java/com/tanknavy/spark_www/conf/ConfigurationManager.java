package com.tanknavy.spark_www.conf;

import java.io.InputStream;
import java.util.Properties;



/*
 * 读取properties，提供外界获取谋改革配置key对应的value的方法
 * scala中可使用com.typesafe.config.ConfigFactory读取配置
 */
public class ConfigurationManager {
	
	private static Properties prop = new Properties();
	 
	static { // 静态代码块，类在ClassLoader加载初始化时会执行一次
	try{ 
			InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties"); //输入流
			prop.load(in); // 从输入流中读取参数
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	public static Integer getInt(String key){
		String value = prop.getProperty(key);
		try{
			return Integer.valueOf(value);
		}catch (Exception e){
			e.printStackTrace();
		}
		return 0;
	}
	
	public static Boolean getBoolean(String key){
		String value = getProperty(key); // 拿到字符串类型是否本机运行
		try{
			return Boolean.valueOf(value);
		}catch(Exception e){
			e.printStackTrace();
		}
		return false;
	}

	public static Long getLong(String key) {
		String value = prop.getProperty(key);
		try{
			return Long.valueOf(value);
		}catch (Exception e){
			e.printStackTrace();
		}
		return 0L;
	}
	
	public static Object getObject(String key){
		String value = prop.getProperty(key);
		try{
			return value.getClass(); // runtime class of this object
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

		
}
