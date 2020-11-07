package com.tanknavy.spark_www.dao;

import java.util.List;


import com.tanknavy.spark_www.domain.AdUserClickCount;

public interface IAdUserClickCountDAO {
	//spark使用updateStateByKey维护全局统计
	void updateBatch(List<AdUserClickCount> adUserClickCounts);
	int findClickCountByKeyDAO(String date,Long userid, Long adid);
	
	//所有记录只管插入，sum来聚合产生黑名单
	void insertBatch(List<AdUserClickCount> adUserClickCounts);
	int findClickCountByKeyDAO_SUM(String date,Long userid, Long adid);
}
