package com.demo.entity;

public class TableStructureEntity {
	
	/**
	 * 字段的名字
	 */
	private String columnName;
	/**
	 * 数据类型
	 */
	private String columnType;
	/**
	 * 字段的长度
	 */
	private int datasize;
	/**
	 * 返回1就表示可以是Null,而0就表示Not Null
	 */
	private int nullable;
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getColumnType() {
		return columnType;
	}
	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}
	public int getDatasize() {
		return datasize;
	}
	public void setDatasize(int datasize) {
		this.datasize = datasize;
	}
	public int getNullable() {
		return nullable;
	}
	public void setNullable(int nullable) {
		this.nullable = nullable;
	}

}
