package com.tanknavy.spark_www.test;

public class StringTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.out.println("-----------------------------");
		StringBuffer keywordsBuffer = new StringBuffer(""); // 拼接字符串,注意null和blank区别
		StringBuffer categoryIDsBuffer = new StringBuffer();
		keywordsBuffer.append("from blank");
		categoryIDsBuffer.append("from null");
		
		System.out.println(keywordsBuffer.toString().contains("ro"));
		System.out.println(keywordsBuffer.toString());
		System.out.println(categoryIDsBuffer.toString());
		
		String tmp = "abc";
		String tmp2 = "abc";
		System.out.println(tmp == tmp2);
		tmp = tmp + "def";
		System.out.println(tmp);
		System.out.println(tmp == "abcdef"); //引用类型比较的是地址
		System.out.println(tmp.equals("abcdef")); // 用equals
		
		
		//final String x = null; //final不可以改变
		String x = null; //final不可以改变
		x= "aa";
		System.out.println(x);
	}

}
