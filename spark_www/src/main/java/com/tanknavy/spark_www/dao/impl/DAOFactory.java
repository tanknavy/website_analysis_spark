package com.tanknavy.spark_www.dao.impl;

import com.tanknavy.spark_www.dao.ISessionAggrStatDAO;
import com.tanknavy.spark_www.dao.ITaskDAO;


/**
 * 为什么要使用工厂设计模式？
 * 如果不使用，比如 DAO dao = new DAOImpl(),如果后面DAO变更了，那么以后所有地方都要更换这个实现类
 * 使用工厂模式 DAO dao = DAOFactory.getDAO()拿到一个dao
 * @author admin
 *
 */
public class DAOFactory {
	
	public static ITaskDAO getTaskDAO(){
		return new ITaskDAOImpl();
	}
	
	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new ISessionAggrStatDAOImpl();
	}
	
}
