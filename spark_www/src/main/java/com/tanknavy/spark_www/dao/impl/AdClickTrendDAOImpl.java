package com.tanknavy.spark_www.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


import com.tanknavy.spark_www.dao.IAdClickTrendDAO;
import com.tanknavy.spark_www.domain.AdClickTrend;
import com.tanknavy.spark_www.jdbc.JDBCHelper;
import com.tanknavy.spark_www.jdbc.JDBCHelper.QueryCallback;
import com.tanknavy.spark_www.model.AdClickTrendQueryResult;

public class AdClickTrendDAOImpl implements IAdClickTrendDAO{

	public void updateBatch(List<AdClickTrend> adClickTrends) {
		// 有就更新，没有就插入
		// 同一个key的数据（比如rdd，包含了多条相同的key，通常在一个分区内）
		// 相同的key假如在不同一个分区，分区并发，可以给一个create_time的时间戳字段
		List<Object[]> updateList = new ArrayList<Object[]>();
		List<Object[]> insertList = new ArrayList<Object[]>();
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		String selectSQL = "SELECT count(*) FROM ad_click_trend WHERE date=? AND hour=? AND minute=? AND ad_id=?";
		
		for(AdClickTrend ad:adClickTrends){
			final AdClickTrendQueryResult queryResult = new AdClickTrendQueryResult();
			Object[] params = new Object[]{ad.getDate(),ad.getHour(),ad.getMinute(),ad.getAdid()};
			jdbcHelper.exeQuery(selectSQL, params, new QueryCallback() { //先查询一下
				
				@Override
				public void process(ResultSet rs) throws Exception {
					if(rs.next()){
						int count = rs.getInt(1);
						queryResult.setCount(count);
					}
					
				}
			});
			//判断是否有结果
			int count = queryResult.getCount();
			if(count > 0){
				updateList.add(new Object[]{ad.getClickCount(),ad.getDate(),ad.getHour(),ad.getMinute(),ad.getAdid()});
			}else{
				insertList.add(new Object[]{ad.getDate(),ad.getHour(),ad.getMinute(),ad.getAdid(),ad.getClickCount()});
			}
		}
		
		//批量更新
		String updateSQL = "UPDATE ad_click_trend SET click_count=? WHERE date=? AND hour=? AND minute=? AND ad_id=?";
		jdbcHelper.exeBatch(updateSQL, updateList);
		
		//批量插入
		String insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)";
		jdbcHelper.exeBatch(insertSQL, insertList);
		
	}
	
}
