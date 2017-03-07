package com.demo.common;

import com.sun.rowset.CachedRowSetImpl;

import javax.sql.RowSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 10:57
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class JdbcHelper {
    /**
     * 获取数据库连接（新）mzb 2016-07-21 16:10:42
     * @param dataSource
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getNewConnection(String dataSource) throws ClassNotFoundException, SQLException {
        Map<String, String> dataSources = Cache.transTables;
        String driver,url,username,password;
        if (dataSource.equals("SQLSERVER")) {
            driver = dataSources.get("DRIVER");
            url = dataSources.get("URL");
            username = dataSources.get("USERNAME");
            password = dataSources.get("PASSWORD");
        }else {
            driver = dataSources.get(dataSource + "_DRIVER");
            url = dataSources.get(dataSource + "_URL");
            username = dataSources.get(dataSource + "_USERNAME");
            password = dataSources.get(dataSource + "_PASSWORD");
        }
        Connection conn = JdbcUnits.getConnection(driver, url, username,password);
        //conn.setAutoCommit(false);
        return conn;
    }

    public static void updateByDb(Connection conn, String sql, String[] fields) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            if (fields != null) {
                for (int i = 0; i < fields.length; i++) {
                    preparedStatement.setObject(i + 1, fields[i].trim());
                }
            }
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new SQLException();
        } finally {
            JdbcUnits.free(conn, preparedStatement, null);
        }

    }

    public static RowSet queryByDb(Connection conn, String sql, String[] fields) throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
            if (fields != null) {
                for (int i = 0; i < fields.length; i++) {
                    preparedStatement.setObject(i + 1, fields[i].trim());
                }
            }
            ResultSet rSet = preparedStatement.executeQuery();
            CachedRowSetImpl rowset = new CachedRowSetImpl();
            rowset.populate(rSet);
            rSet.close();
            return rowset;
        } catch (SQLException e) {
            throw new SQLException(e);
        } finally {
            JdbcUnits.free(conn, preparedStatement, null);
        }
    }
}
