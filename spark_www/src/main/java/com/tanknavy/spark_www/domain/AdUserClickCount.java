package com.tanknavy.spark_www.domain;

public class AdUserClickCount {
	private String date;
	private long userid;
	private long adid;
	private long clickCount;
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public long getUserid() {
		return userid;
	}
	public void setUserid(long userid) {
		this.userid = userid;
	}
	public long getAdid() {
		return adid;
	}
	public void setAdid(long adid) {
		this.adid = adid;
	}
	public long getClickCount() {
		return clickCount;
	}
	public void setClickCount(long clickCount) {
		this.clickCount = clickCount;
	}
	
	
	
}