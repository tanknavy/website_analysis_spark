package com.tanknavy.spark_www.dao;

import java.util.List;

import com.tanknavy.spark_www.domain.SessionDetail;


public interface ISessionDetail {
	void insert(SessionDetail sessionDetail);
	int[] insertBatch(List<SessionDetail> sessionDetailList);
}
