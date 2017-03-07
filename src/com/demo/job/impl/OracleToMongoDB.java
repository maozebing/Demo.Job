package com.demo.job.impl;

import com.demo.common.*;
import com.demo.entity.MongodbTableEntity;
import com.demo.entity.TableStructureEntity;
import com.demo.job.IJob;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import javax.sql.RowSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 20:09
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class OracleToMongoDB implements IJob{

    @Override
    public void execute() {
        System.out.println("导出开始：" + new Date());

        try {
            String startDateStr = "2016-12-01";
            String endDateStr;
            Date startDate = DateUtil.convertToDate(startDateStr);
            Date endDate;

            while (true) {
                startDate = DateUtil.addDateDays(startDate, 1);
                endDate = DateUtil.addDateDays(startDate, 1);
                startDateStr = DateUtil.convertToDateString(startDate);
                endDateStr = DateUtil.convertToDateString(endDate);
                handleAllTableData("ORACLE", "TBL_WASTE_CURRENT_EXIT", startDateStr, endDateStr);
                System.out.println(startDateStr+"导出完成,"+new Date());
                if (startDate == DateUtil.convertToDate("2017-01-01")) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("导出完成：" + new Date());
    }

    @Override
    public void interrupt() {

    }

    /**
     * 一次性导出所有的数据
     *
     * @param dataSource
     * @param tableName
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private void handleAllTableData(String dataSource, String tableName, String startDate, String endDate)
            throws Exception {
        Connection conn = null;

        //查询传输标识为-1的数据
        conn = JdbcHelper.getNewConnection(dataSource);
        String selectSql = String.format("SELECT * FROM %s@its_dbcenter WHERE WORK_DATE>=to_date('%s','yyyy-MM-dd') and  WORK_DATE<to_date('%s','yyyy-MM-dd') and STATION_ID=513", tableName, startDate, endDate);
        RowSet rowset = JdbcHelper.queryByDb(conn, selectSql, null);

        rowset.last();
        int last = rowset.getRow();
        rowset.beforeFirst();

        if (last > 0) {
            List<DBObject> list = new ArrayList<DBObject>();
            conn = JdbcHelper.getNewConnection(dataSource);
            List<TableStructureEntity> tableStructureList = TableUtil.getTableStructureList(conn, tableName, dataSource);
            MongodbTableEntity mongodbTableEntity = Cache.getMongodbTable(tableName);
            Map<String, String> targetSourceList = mongodbTableEntity.getTargetSourceList();
            String columnName;
            //Object columnValue;
            while (rowset.next()) {
                DBObject dbObject = new BasicDBObject();
                for (TableStructureEntity tableStructureEntity : tableStructureList) {
                    columnName = tableStructureEntity.getColumnName();
                    String type = targetSourceList.get(columnName);
                    if (type.equals("DATETIME")) {
                        String columnValue = rowset.getString(tableStructureEntity.getColumnName());
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        dbObject.put(columnName, sdf.parse(columnValue));
                    } else if (type.equals("BIGINT")){
                        double columnValue = rowset.getDouble(tableStructureEntity.getColumnName());
                        dbObject.put(columnName, columnValue);
                    }else if (type.equals("VARCHAR")){
                        String columnValue = rowset.getString(tableStructureEntity.getColumnName());
                        dbObject.put(columnName, columnValue);
                    }else if (type.equals("DOUBLE")){
                        double columnValue = rowset.getDouble(tableStructureEntity.getColumnName());
                        dbObject.put(columnName, columnValue);
                    }
                }
                list.add(dbObject);
                break;
            }
            MongoDBHelper.getInstance().insert("MONGODB", tableName, list);
        }
    }

}
