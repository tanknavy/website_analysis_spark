package com.tanknavy.spark_www.dao;

import com.tanknavy.spark_www.domain.SessionRandomExtract;

/*
 * 随机抽取模块插入mysql表中
 */
public interface ISessionRandomExtractDAO {
	
	void insert(SessionRandomExtract sessionRandomExtract );
}
