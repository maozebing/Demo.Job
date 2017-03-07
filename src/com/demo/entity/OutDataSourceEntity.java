package com.demo.entity;

import java.util.List;


public class OutDataSourceEntity {
	
	private String name;
	private List<OutTableEntity> tableList;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<OutTableEntity> getTableList() {
		return tableList;
	}

	public void setTableList(List<OutTableEntity> tableList) {
		this.tableList = tableList;
	}
	

}
