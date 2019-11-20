package com.tanknavy.spark_www.dao;

import com.tanknavy.spark_www.domain.SessionAggrStat;

/*
 *  接口往domain中插入输入
 */
public interface ISessionAggrStatDAO {
	
	void insert(SessionAggrStat sessionAggrStat);
}
