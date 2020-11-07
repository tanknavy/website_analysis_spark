package com.tanknavy.spark_www.test;

import java.util.Arrays;

import org.hsqldb.lib.IntKeyHashMap;



public class TreeMapSortTest {
	
	public static void main(String args[]){
		int[] arr = {1,5,6,2,9,3};
		System.out.println(Arrays.toString(arr));
		Arrays.sort(arr); // 静态方法，返回void
		System.out.println(Arrays.toString(arr)); //
		
		//模拟输入的iterator
		int[] input = {1,39,5,6,2,9,3,10,20,30};
		
		// 模拟treemap功能，取前N个最大
		// 数据取前10个
		int size = 5;
		int[] topN = new int[size];
		
		for(int z=0;z<input.length;z++){
			//算法开始
			for(int i=0; i<size;i++){
				if (topN[i] == 0 ){ // int默认值为0,这里表示该位置填充值
					topN[i] = input[z]; //空位直接放入
					break; //跳出循环
				}else { // 如果该位置有值
					if(input[z] > topN[i]){ //如果更大，依次往后移动一位
						for(int j = size -1; j>i ;j--){
							topN[j] = topN[j-1];
						}
					topN[i]	 = input[z]; //腾出位置放入更大的新来的
					break;// 更大移动完就结束了，如果更小还会继续循环？
					}
				
				}
			
			}
		}
		
		
		for(int k=0;k<size;k++){
			System.out.println(topN[k]);
		}
		
	}

	
}
