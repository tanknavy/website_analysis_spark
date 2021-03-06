package com.tanknavy.spark_www.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import scala.Char;


public class ListTest {

	public static void main(String[] args) {
		List<Integer> list = new LinkedList<>(); // 为啥实际都一个类型，但是不能有push?
		LinkedList<Integer> link = new LinkedList<>();
		System.out.println(list.getClass().getTypeName());
		System.out.println(link.getClass().getTypeName());
		System.out.println(list.getClass().getTypeName() == link.getClass().getTypeName());
		list.add(1);
		link.push(10);
		
		System.out.println("-----------------------------");
		String[] arr = new String[]{"alex","ben"};
		Object[] params = new Object[]{"alex","ben"};
		System.out.println(arr.getClass().getSimpleName());
		System.out.println(params.getClass().getSimpleName());
		
		System.out.println("-----------------------------");
		final String[] arr2 = new String[5];
		String[] arr3 = new String[5];
		String[] arr4 = new String[10];
		arr2[0] = "Ben";
		arr2[1] = "Alex";
		arr2[0] = "Hero"; //内部元素可以改变
		
		arr3[0] = "Java";
		arr2[3] = arr3[0];
		//arr2 = arr3; //final类型不可以重新赋值
		arr3 = arr4;
		System.out.println(Arrays.toString(arr2));
		
		System.out.println("-----------------------------");
		Long uid = 3518776796426921776L;
		//int uid = 0b1000101;
		String uid2bit = Long.toBinaryString(uid);
		System.out.println(Long.toBinaryString(Long.MAX_VALUE));
		System.out.println(uid2bit);
		System.out.println(uid2bit.length());
		
		System.out.println("-----------------------------");
		StringBuffer keywordsBuffer = new StringBuffer(""); // 拼接字符串,注意null和blank区别
		StringBuffer categoryIDsBuffer = new StringBuffer();
		keywordsBuffer.append("from blank");
		categoryIDsBuffer.append("from null");
		
		System.out.println(keywordsBuffer.toString().contains("ro"));
		System.out.println(keywordsBuffer.toString());
		System.out.println(categoryIDsBuffer.toString());
		
		System.out.println("-----------------------------");
		String num = "123";
		//int str2numA = (int)num; //强制类型转换
		int str2numB = Integer.valueOf(num);
		System.out.println(str2numB + 1);
		
		Random random = new Random();
		System.out.println(random.nextInt(2));
		
		System.out.println("-----------------------------");
		Long t1 = null;
		long t2 =0L ;
		Long[] arr5 = new Long[]{1L,t1,9L,t2};
		System.out.println(arr5[1]);
		System.out.println(arr5[3]);
		
		System.out.println("Integer.valueOf-----------------------------");
		int oldValue = Integer.valueOf(Char.char2int('a'));
		int oldValue2 = Integer.valueOf('b');
		System.out.println(oldValue + 1);
		System.out.println(oldValue2 + 1);
		
		System.out.println("final List-----------------------------");
		
		final List<Integer> sessionList = new ArrayList<>(); // Coolection中元素不能是原始类型，只有Array可以
		for (int i = 0;i <5;i++){
			sessionList.add(i);
			//System.out.println(sessionList[i]); //列表没有index
		}
		Iterator<Integer> it = sessionList.iterator();
		while(it.hasNext()){
			System.out.println(it.next());
		}
		
		String[] arr6 = new String[10];
		arr6[1] = "aa";
		System.out.println(arr6.length);
		System.out.println(arr6[2]);
		
		int[] arr7 = new int[10];
		arr7[1] = 11;
		System.out.println(arr7.length);
		System.out.println(arr7[2]);
		
		String[] arr8 = new String[]{"aa","bb","cc","dd"};
		for(String e:arr8){
			System.out.println(e);
		}
		
		
		
		
		
	}

}
