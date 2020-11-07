package com.tanknavy.spark_www.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


import com.tanknavy.spark_www.dao.IAdUserClickCountDAO;
import com.tanknavy.spark_www.domain.AdUserClickCount;
import com.tanknavy.spark_www.jdbc.JDBCHelper;
import com.tanknavy.spark_www.jdbc.JDBCHelper.QueryCallback;
import com.tanknavy.spark_www.model.AdUserClickCountQueryResult;

public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO{

	public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
		// 先查询，没有就insert,有就update分两类
		List<AdUserClickCount> insertAdUserClickCount = new ArrayList<AdUserClickCount>();
		List<AdUserClickCount> updateAdUserClickCount = new ArrayList<AdUserClickCount>();
		JDBCHelper jdbcHelper = JDBCHelper.getInstance(); //拿到jdbc 单例
		
		String selectSQL = "SELECT count(*) FROM ad_user_click_count WHERE date=? AND user_id=? AND ad_id=?";
		Object[] params = null;
		for(AdUserClickCount adUserClickCount:adUserClickCounts ){
			params = new Object[]{adUserClickCount.getDate(),adUserClickCount.getUserid(),adUserClickCount.getAdid()};
			AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
			
			jdbcHelper.exeQuery(selectSQL, params, new QueryCallback() {
				@Override
				public void process(ResultSet rs) throws Exception {
					if(rs.next())	{
						int count = rs.getInt(1); //取第一条记录的第一个栏位，记录数
						queryResult.setCount(count);
					}
				}
			});
			
			int count = queryResult.getCount();
			if(count >0){
				updateAdUserClickCount.add(adUserClickCount); // 已有记录
			}else{
				insertAdUserClickCount.add(adUserClickCount); //新记录
			}
		}
		
		// 批量插入
		String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";
		List<Object[]> insertParamsList = new ArrayList<>();
		for(AdUserClickCount adUserClickCount:insertAdUserClickCount){
			Object[] insertParams = new Object[]{adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid(),
					adUserClickCount.getClickCount()};
			insertParamsList.add(insertParams);
		}
		jdbcHelper.exeBatch(insertSQL, insertParamsList);
		
		// 批量更新
		String updateSQL = "UPDATE ad_user_click_count SET click_count=? WHERE date=? AND user_id=? AND ad_id=?";
		List<Object[]> updateParamsList = new ArrayList<>();
		for(AdUserClickCount adUserClickCount:updateAdUserClickCount){
			Object[] updateParams = new Object[]{adUserClickCount.getClickCount(),
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid()};
			updateParamsList.add(updateParams);
		}
		jdbcHelper.exeBatch(updateSQL, updateParamsList);
		
	}
	
	// 这个click_count怎么是累积的，每个batch内累积，但是每个key
	public int findClickCountByKeyDAO(String date,Long userid, Long adid){
		String selectSQL = "SELECT click_count FROM ad_user_click_count WHERE date=? AND user_id=? AND ad_id=?";
		Object[] params = new Object[]{date,userid,adid};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance(); //
		
		final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
		jdbcHelper.exeQuery(selectSQL, params, new QueryCallback() { //匿名内部类
			public void process(ResultSet rs) throws Exception {
				if(rs.next()){ // 取第一条记录
					int count = rs.getInt(1); //取第一条记录的第一个栏位，记录数
					queryResult.setCount(count);
				}
			}
		});
		
		int clickCount = queryResult.getCount();
		return clickCount;
		
	}
	
	// 用户点击行为记录全部插入，不更新，下面用sum统计总次数来决定黑名单
	
	public void insertBatch(List<AdUserClickCount> adUserClickCounts) {

		//List<AdUserClickCount> insertAdUserClickCount = new ArrayList<AdUserClickCount>();
		List<Object[]> insertParamsList = new ArrayList<>();
		JDBCHelper jdbcHelper = JDBCHelper.getInstance(); //拿到jdbc 单例
		//Object[] params = null;
		// 批量插入
		String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";

		for(AdUserClickCount adUserClickCount:adUserClickCounts){
			Object[] insertParams = new Object[]{
					adUserClickCount.getDate(),
					adUserClickCount.getUserid(),
					adUserClickCount.getAdid(),
					adUserClickCount.getClickCount()};
			insertParamsList.add(insertParams);
		}
		jdbcHelper.exeBatch(insertSQL, insertParamsList);

	}
	
	
	// 如果没有在spark中维护全局<yyyy-MM-dd,user,ad>点击总次数，这里就在DB中SUN聚集总次数为了生成黑名单
	public int findClickCountByKeyDAO_SUM(String date,Long userid, Long adid){
		String selectSQL = "SELECT sum(click_count) FROM ad_user_click_count "
				+ "WHERE date=? AND user_id=? AND ad_id=? "
				+ "GROUP BY date,user_id,ad_id";
		Object[] params = new Object[]{date,userid,adid};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance(); //
		
		final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();
		jdbcHelper.exeQuery(selectSQL, params, new QueryCallback() { //匿名内部类
			public void process(ResultSet rs) throws Exception {
				if(rs.next()){
					int count = rs.getInt(1); //取第一条记录的第一个栏位 , <yyyy-MM-dd,user,ad>总点击次数
					queryResult.setCount(count);
				}
			}
		});
		
		int clickCount = queryResult.getCount();
		return clickCount;
		
	}


}
