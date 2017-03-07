package com.demo.entity;

import java.util.Map;

public class MongodbTableEntity {
	private String tableName;
	private Map<String, String> targetSourceList;
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public Map<String, String> getTargetSourceList() {
		return targetSourceList;
	}
	public void setTargetSourceList(Map<String, String> targetSourceList) {
		this.targetSourceList = targetSourceList;
	}
	

}
