package com.tanknavy.spark_www.dao.impl;

import java.util.ArrayList;
import java.util.List;

import com.tanknavy.spark_www.dao.ITop10CategoryDAO;
import com.tanknavy.spark_www.domain.Top10Category;
import com.tanknavy.spark_www.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO{

	@Override
	public void insert(Top10Category top10Category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";
		Object[] params = new Object[]{
			top10Category.getTaskid(),
			top10Category.getCategoryid(),
			top10Category.getClickCount(),
			top10Category.getOrderCount(),
			top10Category.getPayCount()
		};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.exeUpdate(sql, params);
		
	}

	@Override
	public int[] insertBatch(List<Top10Category> categoryList) {
		String sql = "insert into top10_category values(?,?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(Top10Category top10Category: categoryList){ // for循环
			Object[] params = new Object[]{ //解析一个domain
					top10Category.getTaskid(),
					top10Category.getCategoryid(),
					top10Category.getClickCount(),
					top10Category.getOrderCount(),
					top10Category.getPayCount()	
			};
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		int[] res = jdbcHelper.exeBatch(sql, paramsList);
		return res;
		
	}

}
