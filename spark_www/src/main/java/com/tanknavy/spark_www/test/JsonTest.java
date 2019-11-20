package com.tanknavy.spark_www.test;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;

import co.cask.tephra.distributed.thrift.TTransactionServer.Processor.resetState;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.tanknavy.spark_www.util.ParamUtils;

/*
 * 阿里和谷歌的json处理
 */

public class JsonTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String json = "[{'name':'github', 'uid':10000, 'site':'www.github.com'},"
				+ "{'name':'runweb', 'uid':20000, 'site':'wwww.google.com'}]"; //注意json的格式
		
		//使用阿里的fastjson
		JSONArray jsonArray = JSONArray.parseArray(json); //解析json串，如果只有一个对象
		JSONObject jsonObject = jsonArray.getJSONObject(1); //第一个对象
		System.out.println("---Alibaba json-------------------------");
		System.out.println(jsonObject.getString("name")); //对应key的值
		System.out.println(jsonObject.getString("site"));
		
		//使用谷歌的gson,
		JsonArray jsonArray2 = new Gson().fromJson(json, JsonArray.class);
		JsonObject jsonObject2 = jsonArray2.get(0).getAsJsonObject();
		JsonObject jsonObject3 = jsonArray2.get(1).getAsJsonObject();
		System.out.println("---Google json-------------------------");
		System.out.println(jsonObject2.get("name")); //注意： 没有getAsString方法会自动加上引号，
		System.out.println(jsonObject2.get("name").getAsString().equals("github"));
		System.out.println(jsonObject3.get("site").getAsString()); //对应key的值
		
		System.out.println("-------------------------");
		String json2 = "[{'name':'github', 'uid':10000, 'site':['www.github.com','www.stackoverflow.com']},"
				+ "{'name':'runweb', 'uid':20000, 'site':'wwww.facebook.com'}]";
		
		JSONArray jsonArray21 = JSONArray.parseArray(json2); //解析json串，如果只有一个对象
		JSONObject jsonObject21 = jsonArray21.getJSONObject(0); //第一个对象
		System.out.println(jsonObject21.getString("site")); 
		
		String test21 = ParamUtils.getParam(jsonObject21, "site");
		System.out.println(test21); 
		
		System.out.println("-------------------------");
		String test22 = getParam_T(jsonObject21, "site");
		System.out.println(test22); 
		
	}
	
		public static String getParam_T(JSONObject jsonObject, String field) {
			JSONArray jsonArray = jsonObject.getJSONArray(field);
			if(jsonArray != null && jsonArray.size() == 1) {
				//return jsonArray.toString();
				return jsonArray.getString(0);
			}
			// json的getString方法www.github.com,www.stackoverflow.com
			else if (jsonArray != null && jsonArray.size() > 1){
				// 参考 ValidUtils.in方法，需要用,拼接
				StringBuffer multElement = new StringBuffer("");
				for (int i=0;i<jsonArray.size();i++){
					multElement.append(jsonArray.getString(i) + ",");
				}
				//去掉最后一个，
				if (multElement.toString().endsWith(",")){
					return  multElement.toString().substring(0, multElement.length() -1);
				}
				
			}
			// toString会得到"www.github.com","www.stackoverflow.com"
			/*
			else if (jsonArray != null && jsonArray.size() > 1){ // ['www.github.com','www.stackoverflow.com']
				//去掉第一个和最后一个字符即可
				if (jsonArray.toString().startsWith("[") && (jsonArray.toString().endsWith("]"))){
					return jsonArray.toString().substring(1, jsonArray.toString().length() -1);
				}
			}*/
			
			
			return null;
		}

}
