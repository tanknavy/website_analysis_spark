package com.tanknavy.spark_www.dao.impl;


import com.tanknavy.spark_www.dao.IAdBlacklistDAO;
import com.tanknavy.spark_www.dao.IAdClickTrendDAO;
import com.tanknavy.spark_www.dao.IAdProvinceTop3DAO;
import com.tanknavy.spark_www.dao.IAdStatDAO;
import com.tanknavy.spark_www.dao.IAdUserClickCountDAO;
import com.tanknavy.spark_www.dao.ISessionAggrStatDAO;
import com.tanknavy.spark_www.dao.ISessionDetail;
import com.tanknavy.spark_www.dao.ISessionRandomExtractDAO;
import com.tanknavy.spark_www.dao.ITaskDAO;
import com.tanknavy.spark_www.dao.ITop10CategoryDAO;
import com.tanknavy.spark_www.dao.ITop10SessionDAO;


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
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
		return new ISessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetail getSessionDetailDAO(){
		return new ISessionDetailImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO(){
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO(){
		return new Top10SessionDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO(){
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO(){
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO(){
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO(){
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO(){
		return new AdClickTrendDAOImpl();
	}
	
}
