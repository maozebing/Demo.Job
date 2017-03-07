package com.demo.entity;

import java.util.List;


public class InTableEntity {
	private String name;
	private String method;
	private List<InTargetSourceEntity> targetSourceList;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getMethod() {
		return method;
	}
	public void setMethod(String method) {
		this.method = method;
	}
	public List<InTargetSourceEntity> getTargetSourceList() {
		return targetSourceList;
	}
	public void setTargetSourceList(List<InTargetSourceEntity> targetSourceList) {
		this.targetSourceList = targetSourceList;
	}	

}
