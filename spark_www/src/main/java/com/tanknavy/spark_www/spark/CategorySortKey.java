package com.tanknavy.spark_www.spark;

import java.io.Serializable;
import scala.math.Ordered;

/**
 * 品类二次排序
 * 封装需要排序笔记的三个字段
 * 实现Ordered接口，和其他相比，如何>,>=,<,<=, compare实现升降序
 * <category,"click=count|order=count|pay=count">
 * @author admin
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey>, Serializable{
	
	private static final long serialVersionUID = 1573144031294007028L;
// 实现接口，泛型是自己,自定义key还要可序列化
	
	// 成员变量用于排序次序
	private long clickCount;
	private long orderCount;
	private long payCount;
	
	

	public CategorySortKey(long clickCount, long orderCount, long payCount) {
		this.clickCount = clickCount;
		this.orderCount = orderCount;
		this.payCount = payCount;
	}

	@Override
	public boolean $greater(CategorySortKey other) { // 当前对象比other大
		if (clickCount > other.getClickCount()){
			return true;
		} else if (clickCount == other.getClickCount() && orderCount > other.getOrderCount()){
			return true;
		} else if (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount > other.getPayCount()) {
			return true;
		}
		return false; //默认其他都是false
	}

	@Override
	public boolean $greater$eq(CategorySortKey other) {
		// 用好上面的，
		if ($greater(other)){
			return true;
		}else if (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public boolean $less(CategorySortKey other) {
		if (clickCount < other.getClickCount()){
			return true;
		} else if (clickCount == other.getClickCount() && orderCount < other.getOrderCount()){
			return true;
		} else if (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount < other.getPayCount()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean $less$eq(CategorySortKey other) {
		if ($less(other)){
			return true;
		}else if (clickCount == other.getClickCount() && orderCount == other.getOrderCount() && payCount == other.getPayCount()){
			return true;
		}
		return false;
	}

	@Override
	public int compare(CategorySortKey other) { // 用于升降序
		if (clickCount - other.getClickCount() != 0){
		return (int)(clickCount - other.getClickCount());
		} else if (orderCount - other.getOrderCount() != 0){
			return (int)(orderCount - other.getOrderCount());
		} else if (payCount - other.getPayCount() != 0){
			return (int)(payCount - other.getPayCount());
		}
		return 0;
	}

	@Override
	public int compareTo(CategorySortKey other) {
		if (clickCount - other.getClickCount() != 0){
			return (int)(clickCount - other.getClickCount());
			} else if (orderCount - other.getOrderCount() != 0){
				return (int)(orderCount - other.getOrderCount());
			} else if (payCount - other.getPayCount() != 0){
				return (int)(payCount - other.getPayCount());
			}
		return 0;
	} 

	
	public long getClickCount() {
		return clickCount;
	}

	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}

	public long getOrderCount() {
		return orderCount;
	}

	public void setOrderCount(long orderCount) {
		this.orderCount = orderCount;
	}

	public long getPayCount() {
		return payCount;
	}

	public void setPayCount(long payCount) {
		this.payCount = payCount;
	}
	

}
