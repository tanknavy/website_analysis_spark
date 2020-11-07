package com.tanknavy.spark_www.dao.impl;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.tanknavy.spark_www.dao.IAdBlacklistDAO;
import com.tanknavy.spark_www.domain.AdBlacklist;
import com.tanknavy.spark_www.jdbc.JDBCHelper;
import com.tanknavy.spark_www.jdbc.JDBCHelper.QueryCallback;

public class AdBlacklistDAOImpl implements IAdBlacklistDAO{

	@Override
	public void insertBatch(List<AdBlacklist> adBlactlists) {
		String sql = "INSERT INTO ad_blacklist VALUES(?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(AdBlacklist adBlacklist: adBlactlists){
			Object[] params = new Object[]{adBlacklist.getUserid()};
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.exeBatch(sql, paramsList);
	}

	@Override
	public List<AdBlacklist> findAllBlacklist() {
		String sql = "SELECT * FROM ad_blacklist";
		List<AdBlacklist> adBlacklists = new ArrayList<>();
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.exeQuery(sql, null, new QueryCallback() { //查询多条数据
			public void process(ResultSet rs) throws Exception {
				while(rs.next()){
					long userid = rs.getLong(1); //第一个栏位
					//long userid2 = Long.valueOf(String.valueOf(rs.getObject(1)));
					AdBlacklist user = new AdBlacklist();
					user.setUserid(userid); //封装到domain
					adBlacklists.add(user); //加入到列表
				}
				
			}
		});
		
		return adBlacklists;
	}

}
