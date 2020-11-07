package com.tanknavy.spark_www.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;



import com.tanknavy.spark_www.conf.ConfigurationManager;
import com.tanknavy.spark_www.constant.Constants;

// 单例方式，设置数据库连接池
public class JDBCHelper { // 避免硬编码
	
	static {
		try {
			String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER); 
			Class.forName(driver);//加载驱动类
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//JDBC单例化？封装一个简单的内部数据库连接池
	private static JDBCHelper instance = null; //自身的实例

	
	public static JDBCHelper getInstance(){
		if (instance == null){
			synchronized(JDBCHelper.class){
				if (instance == null){
					instance = new JDBCHelper(); //保证在整个程序运行中，只会创建一次实例
				}
			}
		}
		return instance;
	}
	
	//private LinkedList<Connection> datasource = null; //如果实例为null,后面无法push
	//private List<Connection> datasource = new LinkedList<>(); //如果这样
	private LinkedList<Connection> datasource = new LinkedList<>(); //连接池对象，初始化
	
	
	private JDBCHelper(){ // 私有构造方法，保证单例模式
		int datasourceSize = ConfigurationManager.getInt(Constants.JDBC_DATASOURCE_SIZE);
		// 创建指定数量的数据库连接，并放入数据库连接池中
		for( int i=0;i<datasourceSize;i++){
			String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			String pass = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			try { //前面Class.forName("com.mysql.jdbc.Driver")已经加载了驱动类
				Connection conn = DriverManager.getConnection(url,user,pass);
				//System.out.println(conn == null); //测试是否有产生连接
				//System.out.println(datasource ==null); //测试是否可以压栈
				datasource.push(conn); //当做stack用
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	//提供获取的数据库连接的方法
	public synchronized Connection getConnection(){ //并发多个线程访问数据库线程池，避免拿到null
		while(datasource.size() == 0){ //线程池暂时没有可用线程的
			try {
				Thread.sleep(10); //10毫秒
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return datasource.poll(); // 有可用的
	}
	
	//增删改
	public int exeUpdate(String sql, Object[] params){
		int rtn = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try{
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			if (params !=null && params.length >0){ //先检查参数不为空切有值才能继续
				for(int i=0;i<params.length;i++){ //参数数组
					pstmt.setObject(i+1, params[i]);
				}
			}
			rtn = pstmt.executeUpdate();
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if (conn != null){
				datasource.push(conn); //连接压回去栈
			}
		}
		
		return rtn; // 返回受影响的行数
	}
	
	// 查询
	//public ResultSet exeQuery(String sql, Object[] params){ //为啥不这样返回结果集
	/**
	 * 
	 * @param sql
	 * @param params
	 * @param callback
	 */
	public void exeQuery(String sql, Object[] params, QueryCallback callback){ 
		//内部类，查询回调接口
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try{
			conn = getConnection();
			pstmt = conn.prepareStatement(sql);
			if (params !=null && params.length >0){
				for(int i=0;i<params.length;i++){
					pstmt.setObject(i+1, params[i]);
				}
			}
			rs = pstmt.executeQuery();
			callback.process(rs);// 引用接口，处理查询结果集
			
		}catch (Exception e){
			e.printStackTrace();
		} finally {
			if (conn != null);
			datasource.push(conn);
		}
		
		//return null;
	}
	
	// 批量查询
	//public void exeBatch(String sql, Object[] params, QueryCallback callback){
	public int[] exeBatch(String sql, List<Object[]> paramList){ // 参数列表,返回每条语句影响的行数
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		//ResultSet rs = null;
		try{
			conn = getConnection();
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement(sql);
			/*
			for(int i=0;i<paramList.size();i++){ //列表直接遍历
				for(int j=0; j<paramList.get(i).length;j++){
					pstmt.setObject(j+1, paramList.get(i)[j]);
				}
			}
			*/
			if (paramList !=null && paramList.size() >0){ //有些无需参数也可以执行
				for(Object[] params: paramList){
					for(int i=0;i<params.length;i++){
						pstmt.setObject(i+1, params[i]);
					}
					pstmt.addBatch(); //批量语句
				}
			}
			
			rtn = pstmt.executeBatch();
			conn.commit();
			//rs = pstmt.executeBatch();
			//callback.process(rs);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if (conn != null){
				datasource.push(conn);
			}
		}
		
		return rtn;
		
	}
	
	public  interface QueryCallback{ //静态内部类：查询回调接口
		void process(ResultSet rs) throws Exception;
	}
	
	
}
