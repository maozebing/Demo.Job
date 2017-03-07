package com.demo.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.demo.entity.*;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

public class XmlReader {

    /**
     * 把导出XML转换成对象
     *
     * @param xmlPath
     */
    public static void getOutRootXml(String xmlPath) {
        try {
            List<OutDataSourceEntity> dataSourceEntities = new ArrayList<OutDataSourceEntity>();
            File file = new File(xmlPath);
            if (file.exists() && !file.isDirectory()) {
                SAXBuilder sx = new SAXBuilder();
                Document doc = sx.build(file);
                Element rootelement = doc.getRootElement();
                List<Element> dataSources = rootelement.getChildren();
                for (int i = 0; i < dataSources.size(); i++) {
                    Element dataSourceElement = dataSources.get(i);
                    if ("DATASOURCE".equals(dataSourceElement.getName().toUpperCase())) {
                        OutDataSourceEntity dataSourceEntity = new OutDataSourceEntity();
                        String dataSourceName = dataSourceElement.getAttribute("name").getValue();
                        dataSourceEntity.setName(dataSourceName.toUpperCase());

                        List<OutTableEntity> tableEntities = new ArrayList<OutTableEntity>();
                        // 获得<DataSource>下的节点
                        List<Element> tableList = dataSourceElement.getChildren();
                        for (int j = 0; j < tableList.size(); j++) {
                            Element tableElement = tableList.get(j);
                            if ("TABLE".equals(tableElement.getName().toUpperCase())) {
                                OutTableEntity tableEntity = new OutTableEntity();
                                String tableName = tableElement.getAttribute("name").getValue();
                                String rows = tableElement.getAttribute("rows").getValue();
                                tableEntity.setName(tableName.toUpperCase());
                                tableEntity.setRows(rows.toUpperCase());
                                tableEntities.add(tableEntity);
                            }
                        }
                        dataSourceEntity.setTableList(tableEntities);
                        dataSourceEntities.add(dataSourceEntity);
                    }
                }
                Cache.outTableList = dataSourceEntities;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 把导入XML转换成对象
     *
     * @param xmlPath
     */
    public static void getInRootXml(String xmlPath) {
        try {
            List<InTableEntity> inTableEntities = new ArrayList<InTableEntity>();
            File file = new File(xmlPath);
            if (file.exists() && !file.isDirectory()) {
                SAXBuilder sx = new SAXBuilder();
                Document doc = sx.build(file);
                Element rootelement = doc.getRootElement();
                List<Element> tables = rootelement.getChildren();
                for (int i = 0; i < tables.size(); i++) {
                    Element tableElement = tables.get(i);
                    if ("TABLE".equals(tableElement.getName().toUpperCase())) {
                        InTableEntity inTableEntity = new InTableEntity();
                        String inTableName = tableElement.getAttribute("name").getValue();
                        String method = tableElement.getAttribute("method").getValue();

                        inTableEntity.setName(inTableName.toUpperCase());
                        inTableEntity.setMethod(method.toUpperCase());

                        List<InTargetSourceEntity> inTargetSourceEntities = new ArrayList<InTargetSourceEntity>();
                        // 获得<Table>下的节点
                        List<Element> targetList = tableElement.getChildren();
                        for (int j = 0; j < targetList.size(); j++) {
                            Element targetElement = targetList.get(j);
                            if ("TARGET".equals(targetElement.getName().toUpperCase())) {
                                InTargetSourceEntity inTargetSourceEntity = new InTargetSourceEntity();

                                String type = targetElement.getAttribute("type").getValue();
                                String target = targetElement.getAttribute("name").getValue();
                                inTargetSourceEntity.setType(type.toUpperCase());
                                if (type.toUpperCase().equals("DB")) {
                                    inTargetSourceEntity.setName(target.toUpperCase());
                                } else {
                                    inTargetSourceEntity.setName(target);
                                }
                                inTargetSourceEntities.add(inTargetSourceEntity);
                            }
                        }
                        inTableEntity.setTargetSourceList(inTargetSourceEntities);
                        inTableEntities.add(inTableEntity);
                    }
                }
                Cache.inTableList = inTableEntities;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 把mongodb数据表XML转换成对象
     *
     * @param xmlPath
     */
    public static void getMongodbRootXml(String xmlPath) {
        try {
            List<MongodbTableEntity> mongodbTableEntities = new ArrayList<MongodbTableEntity>();
            File file = new File(xmlPath);
            if (file.exists() && !file.isDirectory()) {
                SAXBuilder sx = new SAXBuilder();
                Document doc = sx.build(file);
                Element rootelement = doc.getRootElement();
                List<Element> tables = rootelement.getChildren();
                for (int i = 0; i < tables.size(); i++) {
                    Element tableElement = tables.get(i);
                    if ("TABLE".equals(tableElement.getName().toUpperCase())) {
                        MongodbTableEntity mongodbTableEntity = new MongodbTableEntity();
                        String tableName = tableElement.getAttribute("name").getValue();
                        mongodbTableEntity.setTableName(tableName.toUpperCase());

                        Map<String, String> map = new HashMap();
                        // 获得<Table>下的节点
                        List<Element> targetList = tableElement.getChildren();
                        for (int j = 0; j < targetList.size(); j++) {
                            Element targetElement = targetList.get(j);
                            if ("COLUMN".equals(targetElement.getName().toUpperCase())) {
                                String columnName = targetElement.getAttribute("name").getValue();
                                String columnType = targetElement.getAttribute("type").getValue();
                                map.put(columnName.toUpperCase(), columnType.toUpperCase());
                            }
                        }
                        mongodbTableEntity.setTargetSourceList(map);
                        mongodbTableEntities.add(mongodbTableEntity);
                    }
                }
                Cache.mongodbTableList = mongodbTableEntities;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 把任务XML转换成对象
     *
     * @param xmlPath
     * @return
     */
    public static void getJobConfigXml(String xmlPath) {
        try {
            List<JobConfig> jobConfigs = new ArrayList<JobConfig>();
            File file = new File(xmlPath);
            if (file.exists() && !file.isDirectory()) {
                SAXBuilder sx = new SAXBuilder();
                Document doc = sx.build(file);
                Element rootelement = doc.getRootElement();
                List<Element> childs = rootelement.getChildren();
                for (int i = 0; i < childs.size(); i++) {
                    JobConfig jobConfig = new JobConfig();
                    jobConfig.setName(childs.get(i).getChildText("name"));
                    jobConfig.setGroup(childs.get(i).getChildText("group"));
                    if (childs.get(i).getChildText("activity").equals("true")) {
                        jobConfig.setActivity(true);
                    } else {
                        jobConfig.setActivity(false);
                    }
                    jobConfig.setScanPeriod(childs.get(i).getChildText("scanPeriod"));
                    jobConfig.setClassName(childs.get(i).getChildText("className"));
                    jobConfigs.add(jobConfig);
                }
                Cache.jobConfigs = jobConfigs;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
