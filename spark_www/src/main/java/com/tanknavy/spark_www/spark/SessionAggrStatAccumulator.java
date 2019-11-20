package com.tanknavy.spark_www.spark;

import org.apache.spark.AccumulatorParam;
import org.apache.spark.util.AccumulatorV2;

import com.tanknavy.spark_www.constant.Constants;
import com.tanknavy.spark_www.util.StringUtils;

// 自定义Accumulator
// 这里使用自定义的数据格式，String,自定义的类（必须可序列化的），然后基于这种特殊数据格式，
// 可以实现自定义复杂的分布式计算逻辑
public class SessionAggrStatAccumulator implements AccumulatorParam<String>{ // deprecated
//public class SessionAggrStatAccumulator extends AccumulatorV2<IN, OUT>{
	private static final long serialVersionUID = -226487341407235568L;

	@Override
	public String zero(String v) { //数据初始化,各个时间范围区间的统计数量的拼接 count=0|k1=v1|k2=v2|...
		// TODO Auto-generated method stub
		return Constants.SESSION_COUNT + "=0|" 
				+ Constants.TIME_PERIOD_1s_3s + "=0|" 
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|" 
				+ Constants.STEP_PERIOD_1_3+ "=0|" 
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}
	
	// 以下两个方法可以理解为一样的，所以用一个方法add
	// v1是初始化的字符串，v2是遍历session时，判断出某个session对应的时间区间
	// 在v1中找到v2对应的值，累加1，然后更新后的连接串
	@Override
	public String addInPlace(String v1, String v2) {
		// TODO Auto-generated method stub
		return add(v1,v2);
	}


	@Override
	public String addAccumulator(String v1, String v2) {
		// TODO Auto-generated method stub
		return add(v1, v2);
	}
	
	private String add(String v1, String v2) {
		// TODO Auto-generated method stub
		if (StringUtils.isEmpty(v1)){ //没有初始值
			return v2;
		}
		String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		if(oldValue !=null){
			int newValue = Integer.valueOf(oldValue) + 1;
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
		
		return v1; //没有合适的栏位
	}
	
}
