package com.tanknavy.spark_www.dao.impl;

import com.tanknavy.spark_www.dao.ISessionRandomExtractDAO;
import com.tanknavy.spark_www.domain.SessionRandomExtract;
import com.tanknavy.spark_www.jdbc.JDBCHelper;

public class ISessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

	@Override
	public void insert(SessionRandomExtract sessionRandomExtract) { // 插入domain对象
		// TODO Auto-generated method stub
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		Object[] params = new Object[]{ // 要插入的值，可能是不同类型
				sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getSearchKeywords()}; 
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance(); //拿到jdbc连接的单例
		jdbcHelper.exeUpdate(sql, params);
		
	}
	
}
