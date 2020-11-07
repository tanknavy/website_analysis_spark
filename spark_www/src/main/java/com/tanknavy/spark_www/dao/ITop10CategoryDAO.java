package com.tanknavy.spark_www.dao;

import java.util.List;

import com.tanknavy.spark_www.domain.Top10Category;

public interface ITop10CategoryDAO {
	void insert(Top10Category top10Category);
	int[] insertBatch(List<Top10Category> paramList);
}
