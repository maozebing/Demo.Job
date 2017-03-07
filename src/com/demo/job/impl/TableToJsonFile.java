package com.demo.job.impl;

import com.demo.common.*;
import com.demo.entity.OutDataSourceEntity;
import com.demo.entity.OutTableEntity;
import com.demo.entity.TableStructureEntity;
import com.demo.job.IJob;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import javax.sql.RowSet;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 9:32
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class TableToJsonFile implements IJob {

    @Override
    public void execute() {
        System.out.println("导出开始：" + new Date());

        try {
            List<OutDataSourceEntity> outTableList = Cache.outTableList;
            for (OutDataSourceEntity outDataSourceEntity : outTableList) {
                handleOutTable(outDataSourceEntity);
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
     * 处理所有导出表数据
     *
     * @param outDataSourceEntity
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    private void handleOutTable(OutDataSourceEntity outDataSourceEntity)
            throws Exception {
        String dataSource = outDataSourceEntity.getName();

        List<OutTableEntity> outTableEntities = outDataSourceEntity.getTableList();

        for (OutTableEntity outTableEntity : outTableEntities) {
            handleTableData(dataSource, outTableEntity);
        }
    }

    /**
     * 处理一个表数据
     *
     * @param dataSource
     * @param outTableEntity
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    private void handleTableData(String dataSource, OutTableEntity outTableEntity)
            throws Exception {
        String rows = outTableEntity.getRows();
        String tableName = outTableEntity.getName();
        if (rows.equals("ALL")) {
            handleAllTableData(dataSource, tableName);
        } else {
            int count = Integer.parseInt(rows);
            handleBatchTableData(dataSource, tableName, count);
        }
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
    private void handleAllTableData(String dataSource, String tableName)
            throws Exception {
        Connection conn = null;

        // 将传输标识为0的更新为-1
        conn = JdbcHelper.getNewConnection(dataSource);
        String updateSql = String.format("UPDATE %s SET TRANSTAG=-1 WHERE TRANSTAG=0", tableName);
        JdbcHelper.updateByDb(conn, updateSql, null);

        //查询传输标识为-1的数据
        conn = JdbcHelper.getNewConnection(dataSource);
        String selectSql = String.format("SELECT * FROM %s WHERE TRANSTAG=-1", tableName);
        RowSet rowset = JdbcHelper.queryByDb(conn, selectSql, null);

        rowset.last();
        int last = rowset.getRow();
        rowset.beforeFirst();
        if (last > 0) {
            // 生成JSON对象
            JSONObject jsonObject = createJson(rowset, dataSource, tableName);
            if (!jsonObject.get("TOTAL").toString().equals("0")) {
                // 生成文件
                createFile(jsonObject, tableName);
            }

            // 更新数据
            conn = JdbcHelper.getNewConnection(dataSource);
            String updateSql2 = String.format("UPDATE %s SET TRANSTAG=1 WHERE TRANSTAG=-1", tableName);
            JdbcHelper.updateByDb(conn, updateSql2, null);
        }
    }

    /**
     * 按指定行数分批次导出数据
     *
     * @param dataSource
     * @param tableName
     * @param pageSize
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    private void handleBatchTableData(String dataSource, String tableName, int pageSize)
            throws Exception {
        boolean flag = false;
        Connection conn = null;

        while (!flag) {
            // 获取驱动driver
            String driver = "";
            if (dataSource.equals("SQLSERVER")) {
                driver = Cache.transTables.get("DRIVER");
            } else {
                driver = Cache.transTables.get(dataSource + "_DRIVER");
            }

            // 构建查询sql语句
            String selectSql = "";
            String updateSql1 = "";
            String updateSql2 = "";
            if (driver.contains("mysql")) {
                updateSql1 = String.format("UPDATE %s SET TRANSTAG=-1 WHERE TRANSTAG=0 LIMIT %d", tableName, pageSize);
                selectSql = String.format("SELECT * FROM %s WHERE TRANSTAG=-1 LIMIT %d", tableName, pageSize);
                updateSql2 = String.format("UPDATE %s SET TRANSTAG=1 WHERE TRANSTAG=-1", tableName);
            } else if (driver.contains("sqlserver")) {
                updateSql1 = String.format("UPDATE TOP (%d) %s SET TRANSTAG=-1 WHERE TRANSTAG=0", pageSize, tableName);
                selectSql = String.format("SELECT TOP (%d) * FROM %s WHERE TRANSTAG=-1", pageSize, tableName);
                updateSql2 = String.format("UPDATE %s SET TRANSTAG=1 WHERE TRANSTAG=-1", tableName);
            } else if (driver.contains("oracle")) {
                updateSql1 = String.format("UPDATE %s SET TRANSTAG=-1 WHERE TRANSTAG=0 AND ROWNUM<=%d", tableName, pageSize);
                selectSql = String.format("SELECT * FROM TBL_WASTE_CURRENT_EXIT@its_dbcenter WHERE STATION_ID=513 AND TRANSTAG=-1 AND WORK_DATE>=to_date('2017-01-01','yyyy-MM-dd') and WORK_DATE<to_date('2017-02-01','yyyy-MM-dd') AND ROWNUM<=%d", tableName, pageSize);
                updateSql2 = String.format("UPDATE %s SET TRANSTAG=1 WHERE TRANSTAG=-1", tableName);
            }

            // 更新数据,传输标志改为-1
            conn = JdbcHelper.getNewConnection(dataSource);
            JdbcHelper.updateByDb(conn, updateSql1, null);

            // 获取数据
            conn = JdbcHelper.getNewConnection(dataSource);
            RowSet rowset = JdbcHelper.queryByDb(conn, selectSql, null);

            rowset.last();
            int last = rowset.getRow();
            rowset.beforeFirst();
            if (last > 0) {
                // 生成JSON对象
                JSONObject jsonObject = createJson(rowset, dataSource, tableName);
                if (Integer.parseInt(jsonObject.get("TOTAL").toString()) < pageSize) {
                    flag = true;
                }
                if (!jsonObject.get("TOTAL").toString().equals("0")) {
                    // 生成文件
                    createFile(jsonObject, tableName);
                }
                // 更新数据,传输标志改为1
                conn = JdbcHelper.getNewConnection(dataSource);
                JdbcHelper.updateByDb(conn, updateSql2, null);
            }
        }
    }

    /**
     * 生成JSON对象
     *
     * @param rowset
     * @param dataSource
     * @param tableName
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private JSONObject createJson(RowSet rowset, String dataSource, String tableName)
            throws SQLException, ClassNotFoundException {
        JSONObject pJsonObject = new JSONObject();
        pJsonObject.put("TABLENAME", tableName);
        JSONArray jsonArray = new JSONArray();
        Connection conn = JdbcHelper.getNewConnection(dataSource);
        List<TableStructureEntity> tableStructureList = TableUtil.getTableStructureList(conn, tableName, dataSource);
        String columnName;
        Object columnValue;
        while (rowset.next()) {
            JSONObject jsonObject = new JSONObject();
            for (TableStructureEntity tableStructureEntity : tableStructureList) {
                columnName = tableStructureEntity.getColumnName();
                if (columnName.toUpperCase().equals("TRANSTAG")) {
                    columnValue = 1;
                } else {
                    columnValue = rowset.getString(tableStructureEntity.getColumnName());
                }
                jsonObject.put(columnName, columnValue == null ? "" : columnValue);
            }
            jsonArray.add(jsonObject);
        }
        pJsonObject.put("TOTAL", jsonArray.size());
        pJsonObject.put("ROWS", jsonArray);
        return pJsonObject;
    }

    /**
     * 生成文件
     *
     * @param jsonObject
     * @param tableName
     * @throws IOException
     */
    private void createFile(JSONObject jsonObject, String tableName) throws IOException {
        String fileIn = jsonObject.toString();
        String path = getJsonFileName(tableName);
        File filename = new File(path);
        if (!filename.exists()) {
            filename.createNewFile();
        }
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "UTF-8"));
            bufferedWriter.write(fileIn);
            bufferedWriter.close();
        } finally {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        }
        System.out.println(filename + "生成完成:" + new Date());
    }

    /**
     * 获取TXT文件路径
     *
     * @param tableName
     * @return
     */
    private String getJsonFileName(String tableName) {
        String fileName = tableName + "-" + new Date().getTime();
        return Constants.APPLICATION_ROOT_DIR + File.separatorChar + "fromPath" + File.separatorChar + fileName;
    }

}
