package com.tanknavy.spark_www.dao;

import java.util.List;

import com.tanknavy.spark_www.domain.AdStat;

public interface IAdStatDAO {
	//void insertBatch(List<AdStat> adStatList);
	void updateBatch(List<AdStat> adStatList);

}
