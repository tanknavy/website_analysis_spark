package com.tanknavy.spark_www.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;


import com.tanknavy.spark_www.conf.ConfigurationManager;


public class JDBC_CRUD {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		insert();
		select(); // 查询语句ResultSet
		preparedStatement();
	}


	private static void insert()  {
		// TODO Auto-generated method stub
		Connection conn = null;
		Statement stmt = null;
		try{
			// Instances of the class Class represent classes and interfaces in a running Java application
			// A call to forName("X") causes the class named X to be initialized.
			Class.forName("com.mysql.jdbc.Driver"); //反射方法加载驱动类
			
			String dbuser = ConfigurationManager.getProperty("jdbc.user");
			String dbpass = ConfigurationManager.getProperty("jdbc.password");
					
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/www?characterEncoding=utf8",dbuser,dbpass);
			stmt = conn.createStatement();
			String sql = "insert into test(name,age) values('Ben',28)";
			int rtn = stmt.executeUpdate(sql); //返回sql语句影响的行数
			System.out.printf("本次增删改受影响的行数：%s",rtn);
			
			
		} catch(Exception e){
			e.printStackTrace();
		} finally{
			try{
				
				if (stmt != null) {
					stmt.close();
				}
				if (conn !=null){
					conn.close();
				}
				
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}

		
	}
	
	
	private static void select()  {
		// TODO Auto-generated method stub
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try{
			Class.forName("com.mysql.jdbc.Driver"); //反射方法加载驱动类
			
			String dbuser = ConfigurationManager.getProperty("jdbc.user");
			String dbpass = ConfigurationManager.getProperty("jdbc.password");
					
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/www",dbuser,dbpass);
			stmt = conn.createStatement();
			String sql = "select * from test";
			rs = stmt.executeQuery(sql); //返回sql语句影响的行数
			//System.out.printf("%s 行受影响",rtn);
			// 查询结果遍历
			while(rs.next()){
				int id = rs.getInt(1);
				String name = rs.getString(2);
				int age = rs.getInt(3);
				System.out.println("id= " + id + ",name= " + name + ",age= " + age);
			}
			
			
		} catch(Exception e){
			e.printStackTrace();
		} finally{
			try{
				
				if (stmt != null) {
					stmt.close();
				}
				if (conn !=null){
					conn.close();
				}
				
			}catch(Exception e){
				e.printStackTrace();
			}
			
		}
	}
	
	
	//prepare statement: 一是防止sql注入，而是提升性能
	private static void preparedStatement() {
		// TODO Auto-generated method stub
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String dbuser = ConfigurationManager.getProperty("jdbc.user");
			String dbpass = ConfigurationManager.getProperty("jdbc.password");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/www",dbuser,dbpass);
			String sql = "insert into test(name,age) values(?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "Carol"); //每个问题对应的值
			pstmt.setInt(2, 27);
			int rtn = pstmt.executeUpdate();
			System.out.printf("本次增删改受影响的行数：%s",rtn);
			
			
		} catch (Exception e){
			e.printStackTrace();
		}finally {
			try{
				if (pstmt != null){
					pstmt.close();
				}
				if (conn != null){
					conn.close();
				}
				
			}catch(Exception e2){
				e2.printStackTrace();
			}
		}
	
	}

}
