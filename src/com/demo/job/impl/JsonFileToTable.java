package com.demo.job.impl;

import com.demo.common.*;
import com.demo.entity.InTableEntity;
import com.demo.entity.InTargetSourceEntity;
import com.demo.entity.MongodbTableEntity;
import com.demo.entity.TableStructureEntity;
import com.demo.job.IJob;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 11:41
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class JsonFileToTable implements IJob {
    final String path = Constants.APPLICATION_ROOT_DIR + File.separatorChar + "fromPath";

    @Override
    public void execute() {
        try {
            System.out.println("导入开始：" + new Date());

            FileFilter filter = new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    String filename = pathname.getName();
                    return true;
                }
            };

            File filePath = new File(path);
            if (filePath.exists()) {
                File[] files = filePath.listFiles(filter);
                for (File f : files) {
                    handleTempJsonFile(f);
                }
            }
            System.out.println("导入完成：" + new Date());
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void interrupt() {

    }

    /**
     * 处理临时文件夹里的单个文件入库 1、读取txt文件转换为对象 2、将数据对象插入到数据库
     *
     * @param file
     * @throws Exception
     */
    private void handleTempJsonFile(File file) {
        try {
            // 读取文本
            String jsonFileContent = readJsonFile(file);
            // 转JSON对象
            JSONObject transTable = JSONObject.fromObject(jsonFileContent);
            // 获取表名
            String tableName = transTable.getString("TABLENAME");
            // 获取数据
            JSONArray transTableRows = transTable.getJSONArray("ROWS");

            // 获取目标源
            InTableEntity inTableEntity = Cache.getInTable(tableName);
            List<InTargetSourceEntity> inTargetSourceList = inTableEntity.getTargetSourceList();

            // 处理目标源
            for (InTargetSourceEntity inTargetSourceEntity : inTargetSourceList) {
                if ("DB".equals(inTargetSourceEntity.getType())) {
                    if ("MONGODB".equals(inTargetSourceEntity.getName())) {
                        handleMongoDataSource(file, inTargetSourceEntity.getName(), tableName,
                                inTableEntity.getMethod(), transTableRows);
                    } else {
                        handleDataSource(file, inTargetSourceEntity.getName(), tableName, inTableEntity.getMethod(),
                                transTableRows);
                    }
                } else {
                    String destFileName = inTargetSourceEntity.getName() + File.separatorChar + file.getName();
                    FileHelper.copyFile(file.getAbsolutePath(), destFileName);
                }
            }
            file.delete();
        } catch (Exception e) {
            file.delete();
            e.printStackTrace();
        }
    }

    /**
     * 处理mongo数据源
     *
     * @param file
     * @param dataSource
     * @param tableName
     * @param method
     * @param datas
     */
    private void handleMongoDataSource(File file, String dataSource, String tableName, String method, JSONArray datas) {
        try {
            List<DBObject> list = new ArrayList<DBObject>();
            for (int i = 0; i < datas.size(); i++) {
                DBObject dbObject = new BasicDBObject();
                JSONObject data = datas.getJSONObject(i);
                Iterator<String> sIterator = data.keys();
                while (sIterator.hasNext()) {
                    // 获得key
                    String key = sIterator.next();
                    MongodbTableEntity mongodbTableEntity = Cache.getMongodbTable(tableName);
                    Map<String, String> targetSourceList = mongodbTableEntity.getTargetSourceList();
                    String type = targetSourceList.get(key);
                    if (type.equals("DATETIME")) {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        dbObject.put(key, sdf.parse(data.getString(key)));
                    } else {
                        dbObject.put(key, data.get(key));
                    }
                }
                list.add(dbObject);
            }
            MongoDBHelper.getInstance().insert(dataSource, tableName, list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 处理关系型数据库（MYSQL、SQLSERVER、ORACLE）
     *
     * @param file
     * @param dataSource
     * @param tableName
     * @param method
     * @param datas
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private void handleDataSource(File file, String dataSource, String tableName, String method, JSONArray datas) {
        try {
            // 获取数据所有的列名
            JSONObject transTableRow = datas.getJSONObject(0);
            List<String> inTableColumns = new ArrayList<String>();
            Iterator<?> it = transTableRow.keys();
            while (it.hasNext()) {
                String key = it.next().toString();
                inTableColumns.add(key);
            }
            // 获取数据库连接
            Connection conn = JdbcHelper.getNewConnection(dataSource);
            // 获取目标数据表结构
            List<TableStructureEntity> tableStructureList = TableUtil.getTableStructureList(conn, tableName,
                    inTableColumns, dataSource);
            // 获取表主键
            List<String> tableKeyList = TableUtil.getTableKeyList(conn, tableName);
            // 关闭连接
            conn.close();
            // 导入数据
            if ("INSERT".equals(method)) {
                insertData(dataSource, tableName, tableStructureList, datas);
            } else if ("UPDATE".equals(method)) {
                updateData(dataSource, tableName, tableStructureList, tableKeyList, datas);
            } else if ("DELETEALL".equals(method)) {
                deleteAllData(dataSource, tableName, tableStructureList, datas);
            }
        } catch (Exception e) {
            e.printStackTrace();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            String currentDateStr=sdf.format(new Date());
            String destFileName = Constants.APPLICATION_ROOT_DIR + File.separatorChar + "ErrorFiles" + File.separatorChar
                    + currentDateStr + File.separatorChar + dataSource + File.separatorChar + file.getName();
            FileHelper.copyFile(file.getAbsolutePath(), destFileName);
        }
    }

    /**
     * 插入数据 1、进行插入操作，如果有主键冲突跳过
     *
     * @param dataSource
     * @param tableName
     * @param tableStructureList
     * @param transTableRows
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private void insertData(String dataSource, String tableName, List<TableStructureEntity> tableStructureList,
                            JSONArray transTableRows) throws ClassNotFoundException, SQLException {

        String driver = "";
        if (dataSource.equals("SQLSERVER")) {
            driver = Cache.transTables.get("DRIVER");
        } else {
            driver = Cache.transTables.get(dataSource + "_DRIVER");
        }

        String sql = "";
        if (driver.contains("mysql")) {
            sql = TableUtil.getTableMysqlInsertSql(tableName, tableStructureList);
            Connection conn = JdbcHelper.getNewConnection(dataSource);
            conn.setAutoCommit(false);
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int i = 0; i < transTableRows.size(); i++) {
                JSONObject obj = transTableRows.getJSONObject(i);
                for (int j = 0; j < tableStructureList.size(); j++) {
                    String column = tableStructureList.get(j).getColumnName();
                    ps.setObject(j + 1, obj.get(column));
                }
                ps.addBatch();
            }
            try {
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                JdbcUnits.free(conn, ps, null);
            }
        } else {

            sql = TableUtil.getTableInsertSql(tableName, tableStructureList);

            Connection conn = null;
            PreparedStatement ps = null;
            try {
                conn = JdbcHelper.getNewConnection(dataSource);
                ps = conn.prepareStatement(sql);
                for (int i = 0; i < transTableRows.size(); i++) {
                    JSONObject obj = transTableRows.getJSONObject(i);
                    for (int j = 0; j < tableStructureList.size(); j++) {
                        String column = tableStructureList.get(j).getColumnName().toUpperCase();
                        String dataType = tableStructureList.get(j).getColumnType();
                        Object value = obj.get(column) != null ? obj.get(column) : obj.get(column.toLowerCase());
                        if (value.equals("")) {
                            value = null;
                        }
                        if (dataType.equals("DATE")) {
                            if (value.toString().contains(".")) {
                                value = value.toString().substring(0, value.toString().indexOf("."));
                            }
                        }
                        ps.setObject(j + 1, value);
                    }
                    try {
                        ps.executeUpdate();
                        conn.commit();
                    } catch (SQLException e) {
                        continue;

                    }
                }
            }

            finally {
                JdbcUnits.free(conn, ps, null);

            }
        }

    }

    /**
     * 更新数据 1、先进行插入操作，如果有主键冲突 2、再进行更新操作
     *
     * @param dataSource
     * @param tableName
     * @param tableStructureList
     * @param tableKeyList
     * @param transTableRows
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private void updateData(String dataSource, String tableName, List<TableStructureEntity> tableStructureList,
                            List<String> tableKeyList, JSONArray transTableRows) throws ClassNotFoundException, SQLException {
        String sql = TableUtil.getTableInsertSql(tableName, tableStructureList);
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = JdbcHelper.getNewConnection(dataSource);
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < transTableRows.size(); i++) {
                JSONObject obj = transTableRows.getJSONObject(i);
                for (int j = 0; j < tableStructureList.size(); j++) {
                    String column = tableStructureList.get(j).getColumnName().toUpperCase();
                    Object value = obj.get(column) != null ? obj.get(column) : obj.get(column.toLowerCase());
                    ps.setObject(j + 1, value);
                }
                try {
                    ps.executeUpdate();
                } catch (SQLException e) {
                    if (e.getMessage().contains("PRIMARY KEY")) {
                        String updateSql = TableUtil.getTableUpdateSql(tableName, tableStructureList, tableKeyList);
                        PreparedStatement updatePs = conn.prepareStatement(updateSql);
                        for (int k = 0; k < tableStructureList.size(); k++) {
                            String column = tableStructureList.get(k).getColumnName().toUpperCase();
                            Object value = obj.get(column) != null ? obj.get(column) : obj.get(column.toLowerCase());

                            updatePs.setObject(k + 1, value);
                        }
                        for (int m = 0; m < tableKeyList.size(); m++) {
                            updatePs.setObject(tableStructureList.size() + m + 1, obj.get(tableKeyList.get(m)));
                        }
                        updatePs.executeUpdate();
                    }

                    continue;
                }
            }
        } finally {
            JdbcUnits.free(conn, ps, null);
        }
    }

    /**
     * 先清除数据再插入数据 1、先清除目标表数据 2、再插入数据
     *
     * @param dataSource
     * @param tableName
     * @param tableStructureList
     * @param transTableRows
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private void deleteAllData(String dataSource, String tableName, List<TableStructureEntity> tableStructureList,
                               JSONArray transTableRows) throws ClassNotFoundException, SQLException {
        String deleteSql = String.format("DELETE FROM %s", tableName);
        Connection conn = JdbcHelper.getNewConnection(dataSource);
        JdbcHelper.updateByDb(conn, deleteSql, null);

        String sql = TableUtil.getTableInsertSql(tableName, tableStructureList);
        conn = JdbcHelper.getNewConnection(dataSource);
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement(sql);
        for (int i = 0; i < transTableRows.size(); i++) {
            JSONObject obj = transTableRows.getJSONObject(i);
            for (int j = 0; j < tableStructureList.size(); j++) {
                String column = tableStructureList.get(j).getColumnName();
                Object value = obj.get(column);
                ps.setObject(j + 1, value);
            }
            ps.addBatch();
        }

        try {
            ps.executeBatch();
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            JdbcUnits.free(conn, ps, null);
        }
    }


    /**
     * 读取json文本文件内容
     *
     * @param file
     * @return
     * @throws IOException
     */
    private String readJsonFile(File file) throws IOException {

        StringBuilder builder = new StringBuilder();
        InputStreamReader read = null;
        BufferedReader reader = null;
        read = new InputStreamReader(new FileInputStream(file), "UTF-8");
        reader = new BufferedReader(read);
        String str = null;
        while ((str = reader.readLine()) != null) {
            builder.append(str);
        }
        reader.close();
        return builder.toString();
    }


}
