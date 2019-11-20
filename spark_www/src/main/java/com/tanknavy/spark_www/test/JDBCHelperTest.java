package com.tanknavy.spark_www.test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tanknavy.spark_www.jdbc.JDBCHelper;

public class JDBCHelperTest {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		JDBCHelper helper = JDBCHelper.getInstance(); //单例
		//helper.exeUpdate("insert into test(name,age) values(?,?)", ["Kevin", 23]); // java会将数据分开作为每个参数
		//helper.exeUpdate("insert into test(name,age) values(?,?)", new Object[]{"zeo",23}); //exeUpdate(String, Object[])
		
		// 测试带有回调的查询
		final Map<String, Object[]> testUser = new HashMap<>(); // 必须为final,变量不可以重新赋值，内部元素可以改变
		helper.exeQuery(
				"select * from test where name = ?", 
				new Object[]{"Alex"}, 
				new JDBCHelper.QueryCallback() { //匿名内部类封装了我们本地对的处理逻辑
					@Override
					public void process(ResultSet rs) throws Exception {
						// TODO Auto-generated method stub
						if (rs.next()){
							String uid = rs.getString(1);
							String name = rs.getString(2);
							int age = rs.getInt(3);
							testUser.put(uid, new Object[]{name,age});
							//testUser.put("name", name); //匿名内部内如果要使用外部类中的一些成员，必须申明为final类型
							//testUser.put("age", age);
						}
					}
				}); 
		for(Entry<String, Object[]> m: testUser.entrySet()){
			System.out.println(m.getKey() + ":" + Arrays.toString(m.getValue()));
		}
		
		//测试批量查询
		String sql = "insert into test(name,age) values(?,?)";
		List<Object[]> list = new ArrayList<>();
		list.add(new Object[]{"gigi",20}); 
		list.add(new Object[]{"vivi",19});
		list.add(new Object[]{"kiki",19});
		list.add(new Object[]{"大佬",39});
		helper.exeBatch(sql, list);
		
	}

}
