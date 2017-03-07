package com.demo.common;

import java.util.*;

import com.demo.entity.InTableEntity;
import com.demo.entity.JobConfig;
import com.demo.entity.MongodbTableEntity;
import com.demo.entity.OutDataSourceEntity;

public class Cache {

	//数据源配置
	public static Map<String, String> transTables= Collections.synchronizedMap(new HashMap<String, String>());

	//数据库导入文件XML配置
	public static List<OutDataSourceEntity> outTableList=new ArrayList<OutDataSourceEntity>();

	//文件导入数据库XML配置
	public static List<InTableEntity> inTableList=new ArrayList<InTableEntity>();

	//mongodb表XML配置
	public static List<MongodbTableEntity> mongodbTableList=new ArrayList<MongodbTableEntity>();

	//任务XML配置
	public static List<JobConfig> jobConfigs=new ArrayList<JobConfig>();


	/**
	 * 根据表名获取导入表对象
	 * @param tableName
	 * @return
	 */
	public static InTableEntity getInTable(String tableName){
		InTableEntity inTable =null;
		for (InTableEntity inTableEntity : inTableList) {
			if (tableName.equals(inTableEntity.getName())) {
				inTable=inTableEntity;
				break;
			}
		}
		return inTable;
	}

	public static MongodbTableEntity getMongodbTable(String tableName){
		MongodbTableEntity tableEntity =null;
		for (MongodbTableEntity mongodbTableEntity : mongodbTableList) {
			if (tableName.equals(mongodbTableEntity.getTableName())) {
				tableEntity=mongodbTableEntity;
				break;
			}
		}
		return tableEntity;
	}

}
