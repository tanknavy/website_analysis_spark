package com.tanknavy.spark_www.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;






import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;

import com.tanknavy.spark_www.dao.IAdStatDAO;
import com.tanknavy.spark_www.domain.AdStat;
import com.tanknavy.spark_www.jdbc.JDBCHelper;
import com.tanknavy.spark_www.jdbc.JDBCHelper.QueryCallback;
import com.tanknavy.spark_www.model.AdStatQueryResult;

public class AdStatDAOImpl implements IAdStatDAO {
	
	/*
	public void insertBatch(List<AdStat> adStatList) {
		String sql = "INSERT INTO ad_stat VALUES(?,?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		
		for(AdStat adStat:adStatList){
			String date = adStat.getDate();
			String province = adStat.getProvince();
			String city = adStat.getCity();
			long adid = adStat.getAdid();
			long clickCount = adStat.getClickCount();
			
			Object[] params = new Object[]{date,province,city,adid,clickCount};
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.exeBatch(sql, paramsList);
		
	}*/
	
	
	public void updateBatch(List<AdStat> adStatList) {
		String selectSQL = "SELECT count(*) FROM ad_stat WHERE date=? AND province=? AND city=? AND ad_id=?";
		String insertSQL = "INSERT INTO ad_stat VALUES(?,?,?,?,?)";
		String updateSQL = "UPDATE ad_stat set click_count=? WHERE date=? AND province=? AND city=? AND ad_id=?";
		
		List<Object[]> insertParamsList = new ArrayList<Object[]>();
		List<Object[]> updateParamsList = new ArrayList<Object[]>();
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();

		
		for(AdStat adStat:adStatList){
			final AdStatQueryResult adStatQueryResult = new AdStatQueryResult(); // model
			Object[] params = new Object[]{adStat.getDate(),adStat.getProvince(),adStat.getCity(),adStat.getAdid()};
			jdbcHelper.exeQuery(selectSQL, params, new QueryCallback() {
				
				@Override
				public void process(ResultSet rs) throws Exception {
					if(rs.next()){ // 有结果
						int count = rs.getInt(1); //取第一个栏位获取记录数
						adStatQueryResult.setCount(count);
						//updateParamsList.add(new Object[]{}); //mysql中已经有记录了
					}
					//insertParamsList.add(new Object[]{}); //mysql中没有记录了
				}
			});
			
			int count = adStatQueryResult.getCount();
			if(count > 0){
				updateParamsList.add(new Object[]{adStat.getClickCount(), adStat.getDate(), adStat.getProvince(), adStat.getCity(), adStat.getAdid()});
			}else{
				insertParamsList.add(new Object[]{adStat.getDate(), adStat.getProvince(), adStat.getCity(), adStat.getAdid(), adStat.getClickCount()});
			}
		}
		
		jdbcHelper.exeBatch(insertSQL, insertParamsList);//批量插入
		jdbcHelper.exeBatch(updateSQL, updateParamsList);//批量更新
		
	}



}
