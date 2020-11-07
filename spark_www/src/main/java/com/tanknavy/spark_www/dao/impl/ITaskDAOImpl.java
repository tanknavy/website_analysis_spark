package com.tanknavy.spark_www.dao.impl;

import java.sql.ResultSet;

import com.tanknavy.spark_www.dao.ITaskDAO;
import com.tanknavy.spark_www.domain.Task;
import com.tanknavy.spark_www.jdbc.JDBCHelper;


public class ITaskDAOImpl implements ITaskDAO{

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public Task findById(long taskid) {
		// TODO Auto-generated method stub
		final Task task = new Task();
		String sql = "select * from task where task_id=?";
		Object[] params = new Object[]{taskid};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.exeQuery(sql, params, new JDBCHelper.QueryCallback() { // 查询回调，匿名内部类实现
			
			@Override
			public void process(ResultSet rs) throws Exception {
				// TODO Auto-generated method stub
				if(rs.next()){
					long taskid = rs.getLong(1);
					String taskName = rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finishTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatus = rs.getString(7);
					String taskParam = rs.getString(8);
					
					task.setTaskid(taskid); //都封装到对象里面
					task.setTaskName(taskName); 
					task.setCreateTime(createTime); 
					task.setStartTime(startTime);
					task.setFinishTime(finishTime);
					task.setTaskType(taskType);  
					task.setTaskStatus(taskStatus);
					task.setTaskParam(taskParam); 
				}
			}
		});
		return task;
		/**
		 * 用JDBC进行数据库操作，最大的问题就是麻烦，为了查询某些数据，需要数据获取，数据的设置，Domain对象的封装
		 * 可以用MyBatis/Hibernate
		 */
	}

}
