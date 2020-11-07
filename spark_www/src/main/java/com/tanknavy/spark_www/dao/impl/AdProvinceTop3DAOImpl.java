package com.tanknavy.spark_www.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


import com.tanknavy.spark_www.dao.IAdProvinceTop3DAO;
import com.tanknavy.spark_www.domain.AdProvinceTop3;
import com.tanknavy.spark_www.jdbc.JDBCHelper;
import com.tanknavy.spark_www.jdbc.JDBCHelper.QueryCallback;
import com.tanknavy.spark_www.model.AdProvinceTop3QueryResult;

public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO{
	

	//top3，已有的直接删除，然后插入
	@Override
	public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {
	
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		//每天每省份的top,所以先按照date_province去重，方便后面的删除
		List<String> dateProvinces = new ArrayList<>();
		for(AdProvinceTop3 ad:adProvinceTop3s){
			String date = ad.getDate();
			String province = ad.getProvince();
			String key = date + "_" + province;
			if(!dateProvinces.contains(key)){
				dateProvinces.add(key);
			}
		}
		
		//批量删除DB中相同的date_province
		String deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?";
		List<Object[]> deletetList = new ArrayList<Object[]>();
		for(String dateProvince: dateProvinces){
			//AdProvinceTop3 ad = new AdProvinceTop3();
			String[] splitted = dateProvince.split("_");
			String date = splitted[0];
			String province = splitted[1];
			Object[] params = new Object[]{date,province};
			deletetList.add(params);
		}
		jdbcHelper.exeBatch(deleteSQL, deletetList);
		
		//批量插入最新传入进来的所有数据；
		String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";
		List<Object[]> insertList = new ArrayList<Object[]>();
		for(AdProvinceTop3 ad:adProvinceTop3s){
			Object[] params = new Object[]{ad.getDate(), ad.getProvince(), ad.getAdid(), ad.getClickCount()};
			insertList.add(params);
		}
		jdbcHelper.exeBatch(insertSQL, insertList);
		
	}
	
	
	public void updateBatch2(List<AdProvinceTop3> adProvinceTop3s) {
		// 有记录就更新，没有就插入
		List<Object[]> insertList = new ArrayList<Object[]>();
		List<Object[]> updateList = new ArrayList<Object[]>();
		String selectSQL = "SELECT COUNT(*) FROM ad_province_top3 WHERE date=? AND province=? AND ad_id=? AND click_count=?";
		String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";
		String updateSQL = "UPDATE ad_province_top3 SET click_count=? WHERE date=? AND province=? AND ad_id=? AND click_count=?";
		AdProvinceTop3QueryResult result = new AdProvinceTop3QueryResult();
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		for(AdProvinceTop3 ad:adProvinceTop3s){
			//AdProvinceTop3 ad = new AdProvinceTop3();
			Object[] params = new Object[]{ad.getDate(), ad.getProvince(), ad.getAdid(), ad.getClickCount()};
			 //查询结构集由callback处理
			jdbcHelper.exeQuery(selectSQL, params, new QueryCallback() {

				public void process(ResultSet rs) throws Exception {
					if(rs.next()){
						int QueryResult = rs.getInt(1);
						result.setCount(QueryResult);
					}
					
				}
			});
			
			if(result.getCount() > 0){
				updateList.add(new Object[]{ad.getClickCount(),ad.getDate(),ad.getProvince(),ad.getAdid()});
			}else{
				insertList.add(new Object[]{ad.getDate(),ad.getProvince(),ad.getAdid(),ad.getClickCount()});
			}
		}
		
		jdbcHelper.exeBatch(updateSQL, updateList);
		jdbcHelper.exeBatch(insertSQL, insertList);
		
	}



}
