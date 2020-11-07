package com.tanknavy.spark_www.dao;

import java.util.List;

import com.tanknavy.spark_www.domain.AdProvinceTop3;

public interface IAdProvinceTop3DAO {
	void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
