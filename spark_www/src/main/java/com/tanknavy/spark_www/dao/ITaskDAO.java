package com.tanknavy.spark_www.dao;

import com.tanknavy.spark_www.domain.Task;

/*
 * Task接口
 */

public interface ITaskDAO {
	Task findById(long taskid); //根据主键查询任务
}
