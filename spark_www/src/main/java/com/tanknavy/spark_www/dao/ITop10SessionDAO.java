package com.tanknavy.spark_www.dao;

import java.util.List;

import com.tanknavy.spark_www.domain.Top10Session;

public interface ITop10SessionDAO {
	void insert(Top10Session top10Session);
	int[] insertBatch(List<Top10Session> paramList);
}
