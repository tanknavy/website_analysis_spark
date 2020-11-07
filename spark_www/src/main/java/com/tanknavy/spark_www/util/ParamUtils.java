package com.tanknavy.spark_www.util;

import org.apache.hadoop.hive.ql.parse.HiveParser.ifExists_return;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.tanknavy.spark_www.conf.ConfigurationManager;
import com.tanknavy.spark_www.constant.Constants;

/**
 * 参数工具类
 * @author Administrator
 *
 */
public class ParamUtils {

	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args) {
		try {
			if(args != null && args.length > 0) {
				return Long.valueOf(args[0]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}  
		return null;
	}
	
	// 本地模式在配置文件中指定taskid,生产模式从命令行读取
	public static Long getTaskIdFromArgsLocal(String[] args, String taskType) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			return ConfigurationManager.getLong(taskType);
		}else{
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}  
		}
		
		return null;
	}
	
	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject JSON对象
	 * @return 参数
	 */
	public static String getParam(JSONObject jsonObject, String field) {
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		//if(jsonArray != null && jsonArray.size() > 0) {
		if(jsonArray != null && jsonArray.size() == 1) { //和下面的else if联合判断
			return jsonArray.getString(0); //假如 {age: 20, 'cities':['shanghai','bei']},只能返回第一个值
			//return jsonArray.toString();
		}
		// 注意：为了读取'cities':['shanghai','bei']格式,这种方式导致解析出来的字符是cities="city72","city5"
		/* 
		else if (jsonArray != null && jsonArray.size() > 1){ 
			//根据实际写入格式，去掉第一个和最后一个字符即可, 
			//if (jsonArray.toString().startsWith("[") && (jsonArray.toString().endsWith("]"))){ //注意写入格式
				return jsonArray.toString().substring(1, jsonArray.toString().length() -1);
			//}
		}*/
		else if (jsonArray != null && jsonArray.size() > 1){ //getString方法保证解析出来没有引号cities=city72,city5
			// 参考 ValidUtils.in方法，需要用,拼接
			StringBuffer arr = new StringBuffer("");
			for (int i=0;i<jsonArray.size();i++){
				arr.append(jsonArray.getString(i) + ","); //赤裸裸的原始字符串在缓冲区中
				//arr.append(jsonArray.getString(i)); //逻辑没错，但是发现无法正确解析, 反复测试无法理解
				//if (i < jsonArray.size() - 1){
				//	arr.append(","); 
				//}
			}
			//去掉最后一个，更高效的办法是在前面判断i< length -1时才追加,
			if (arr.toString().endsWith(",")){
				return  arr.toString().substring(0, arr.length() -1);
			}
		}
		
		return null;
	}
	
	
	public static String getParam2(JsonObject jsonObject, String field) {
		JsonArray jsonArray = jsonObject.getAsJsonArray(field);
		if(jsonArray != null && jsonArray.size() > 0) {
			return jsonArray.getAsString();
		}
		return null;
	}
	
}
