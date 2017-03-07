package com.demo.common;

import com.demo.entity.TableStructureEntity;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 11:04
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class TableUtil {
    /**
     * 获取数据表结构
     *
     * @param conn
     * @param tableName
     * @return
     */
    public static List<TableStructureEntity> getTableStructureList(Connection conn, String tableName,String dataSource) {
        try {
            String userName=Cache.transTables.get(dataSource+"_USERNAME");;
            ResultSet rs = conn.getMetaData().getColumns(null, userName, tableName, null);
            List<TableStructureEntity> tableStructureList = new ArrayList<TableStructureEntity>();
            String columnName;
            String columnType;
            int datasize;
            int nullable;
            while (rs.next()) {
                TableStructureEntity entity = new TableStructureEntity();
                columnName = rs.getString("COLUMN_NAME");
                columnType = rs.getString("TYPE_NAME");
                datasize = rs.getInt("COLUMN_SIZE");
                nullable = rs.getInt("NULLABLE");
                entity.setColumnName(columnName);
                entity.setColumnType(columnType);
                entity.setDatasize(datasize);
                entity.setNullable(nullable);
                tableStructureList.add(entity);
            }
            return tableStructureList;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取数据表结构
     *
     * @param conn
     * @param tableName
     * @param inTableColumns
     * @return
     * @throws SQLException
     */
    public static List<TableStructureEntity> getTableStructureList(Connection conn, String tableName,
                                                                   List<String> inTableColumns,String dataSource) throws SQLException {
        try {
            String userName=userName=Cache.transTables.get(dataSource+"_USERNAME");;
            ResultSet rs = conn.getMetaData().getColumns(null, userName, tableName, null);
            List<TableStructureEntity> tableStructureList = new ArrayList<TableStructureEntity>();
            String columnName;
            String columnType;
            int datasize;
            int nullable;

            while (rs.next()) {
                columnName = rs.getString("COLUMN_NAME").toUpperCase();
                columnType = rs.getString("TYPE_NAME");
                datasize = rs.getInt("COLUMN_SIZE");
                nullable = rs.getInt("NULLABLE");

                for (String inTableColumn : inTableColumns) {
                    if (columnName.equals(inTableColumn.toUpperCase())) {
                        TableStructureEntity entity = new TableStructureEntity();
                        entity.setColumnName(columnName);
                        entity.setColumnType(columnType);
                        entity.setDatasize(datasize);
                        entity.setNullable(nullable);
                        tableStructureList.add(entity);
                        break;
                    }
                }
            }
            return tableStructureList;
        } catch (SQLException e) {
            throw e;
        }
    }

    /**
     * 获取数据表主键
     *
     * @param conn
     * @param tableName
     * @return
     */
    public static List<String> getTableKeyList(Connection conn, String tableName) {
        try {
            ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, tableName);
            List<String> keyList = new ArrayList<String>();
            while (rs.next()) {
                keyList.add(rs.getString("COLUMN_NAME").toUpperCase());
            }
            return keyList;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取数据表插入sql
     * @param tableName 表名
     * @param tableStructureList 目标表结构
     * @return
     */
    public static String getTableInsertSql(String tableName,List<TableStructureEntity> tableStructureList){
        String sql="INSERT INTO "+tableName+" (";
        String insertValue=") values (";

        for (TableStructureEntity tableStructureEntity : tableStructureList) {
            String column=tableStructureEntity.getColumnName();
            sql=sql+column+",";
            insertValue=insertValue+"?,";
        }

        if (tableStructureList.size()>0) {
            sql=sql.substring(0,sql.length()-1);
            insertValue=insertValue.substring(0,insertValue.length()-1);
        }

        insertValue=insertValue+")";
        sql=sql+insertValue;

        return sql;
    }

    /**
     * 获取MYSQL数据表批量插入sql，主键冲突忽略
     * @param tableName 表名
     * @param tableStructureList 目标表结构
     * @return
     */
    public static String getTableMysqlInsertSql(String tableName,List<TableStructureEntity> tableStructureList){
        String sql="INSERT IGNORE INTO "+tableName+" (";
        String insertValue=") values (";

        for (TableStructureEntity tableStructureEntity : tableStructureList) {
            String column=tableStructureEntity.getColumnName();
            sql=sql+column+",";
            insertValue=insertValue+"?,";
        }

        if (tableStructureList.size()>0) {
            sql=sql.substring(0,sql.length()-1);
            insertValue=insertValue.substring(0,insertValue.length()-1);
        }

        insertValue=insertValue+")";
        sql=sql+insertValue;

        return sql;
    }

    /**
     * 获取更新sql
     * @param tableName
     * @param tableStructureList
     * @param tableKeyList
     * @return
     */
    public static String getTableUpdateSql(String tableName, List<TableStructureEntity> tableStructureList, List<String> tableKeyList){
        String sql="UPDATE "+tableName+" SET ";
        String where=" WHERE ";

        for (TableStructureEntity tableStructureEntity : tableStructureList) {
            String column=tableStructureEntity.getColumnName();
            sql=sql+column+"=?,";
        }

        for (String string : tableKeyList) {
            where=where+string+"=? AND ";
        }

        if (tableStructureList.size()>0) {
            sql=sql.substring(0,sql.length()-1);
        }

        if (tableKeyList.size()>0) {
            where=where.substring(0,where.length()-4);
        }

        sql=sql+where;
        return sql;
    }
}
