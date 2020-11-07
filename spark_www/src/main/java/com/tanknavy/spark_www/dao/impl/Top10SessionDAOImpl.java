package com.tanknavy.spark_www.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.intervalLiteral_return;

import com.tanknavy.spark_www.dao.ITop10SessionDAO;
import com.tanknavy.spark_www.domain.Top10Category;
import com.tanknavy.spark_www.domain.Top10Session;
import com.tanknavy.spark_www.jdbc.JDBCHelper;

public class Top10SessionDAOImpl implements ITop10SessionDAO{

	@Override
	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_category_session values(?,?,?,?)"; 
		
		Object[] params = new Object[]{
				top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.exeUpdate(sql, params);
		
	}

	@Override
	public int[] insertBatch(List<Top10Session> sessionList) {
		String sql = "insert into top10_category_session values(?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(Top10Session top10Session: sessionList){ // for循环
			Object[] params = new Object[]{ //解析一个domain
					top10Session.getTaskid(),
					top10Session.getCategoryid(),
					top10Session.getSessionid(),
					top10Session.getClickCount()
			};

			paramsList.add(params);
			
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		int[] res = jdbcHelper.exeBatch(sql, paramsList);
		return res;
		
	}

}
