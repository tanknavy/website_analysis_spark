package com.tanknavy.spark_www.dao;

import java.util.List;

import com.tanknavy.spark_www.domain.AdClickTrend;

public interface IAdClickTrendDAO {
	
	void updateBatch(List<AdClickTrend> adClickTrends);
}
