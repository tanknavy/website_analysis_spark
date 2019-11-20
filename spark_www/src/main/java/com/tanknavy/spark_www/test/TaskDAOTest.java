package com.tanknavy.spark_www.test;

import com.tanknavy.spark_www.dao.ITaskDAO;
import com.tanknavy.spark_www.dao.impl.DAOFactory;
import com.tanknavy.spark_www.domain.Task;

public class TaskDAOTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());
	}

}
