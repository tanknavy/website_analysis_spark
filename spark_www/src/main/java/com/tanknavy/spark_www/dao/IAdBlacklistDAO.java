package com.tanknavy.spark_www.dao;

import java.util.List;

import com.tanknavy.spark_www.domain.AdBlacklist;

public interface IAdBlacklistDAO {
	
	//批量插入广告黑名单
	void insertBatch(List<AdBlacklist> adBlactlists);
	
	// 查找黑名单用户
	List<AdBlacklist> findAllBlacklist();
}
