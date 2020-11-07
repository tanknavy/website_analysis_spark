package com.tanknavy.spark_www.dao.impl;

import java.util.ArrayList;
import java.util.List;
import com.tanknavy.spark_www.dao.ISessionDetail;
import com.tanknavy.spark_www.domain.SessionDetail;
import com.tanknavy.spark_www.jdbc.JDBCHelper;

public class ISessionDetailImpl implements ISessionDetail{

	@Override
	public void insert(SessionDetail sessionDetail) {
		// TODO Auto-generated method stub
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[]{
				sessionDetail.getTaskid(),
				sessionDetail.getUserid(),
				sessionDetail.getSessionid(),
				sessionDetail.getPageid(),
				sessionDetail.getActionTime(),
				sessionDetail.getSearchKeyword(),
				sessionDetail.getClickCategoryId(),
				sessionDetail.getClickProductId(),
				sessionDetail.getOrderCategoryIds(),
				sessionDetail.getOrderProductIds(),
				sessionDetail.getPayCategoryIds(),
				sessionDetail.getPayProductIds()
		};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.exeUpdate(sql, params);
	}
	
	// 可以批量插入
	public int[] insertBatch(List<SessionDetail> sessionDetails){ //还是要循环取数
		String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
		List<Object[]> paramsList = new ArrayList<Object[]>();
		for(SessionDetail sessionDetail: sessionDetails){ // for循环
			Object[] params = new Object[]{ //解析一个domain
					sessionDetail.getTaskid(),
					sessionDetail.getUserid(),
					sessionDetail.getSessionid(),
					sessionDetail.getPageid(),
					sessionDetail.getActionTime(),
					sessionDetail.getSearchKeyword(),
					sessionDetail.getClickCategoryId(),
					sessionDetail.getClickProductId(),
					sessionDetail.getOrderCategoryIds(),
					sessionDetail.getOrderProductIds(),
					sessionDetail.getPayCategoryIds(),
					sessionDetail.getPayProductIds()	
			};
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		int[] res = jdbcHelper.exeBatch(sql, paramsList);
		return res;
	}

	
}
