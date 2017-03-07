package com.demo.common;

import java.sql.*;

/**
 * 描述 ：
 *
 * @author : maozebing
 * @version : v1.00
 * @CreationDate : 2017/3/1 10:58
 * @Description :
 * @update : 修改人，修改时间，修改内容
 * @see :[相关类/方法]
 */
public class JdbcUnits {
    /**
     * Oracle日间格式化通用解决方方法
     * @param cn
     * @throws SQLException
     */
    public static void oracleDateFormat(Connection cn) throws SQLException {
        if (cn.toString().indexOf("oracle")>=0){
            PreparedStatement ps = cn.prepareStatement("alter session set nls_date_format='YYYY-MM-DD HH24:mi:ss'");
            ps.execute();
            ps.close();
        }
    }

    /**
     * 获取数据库连接
     * @param driver
     * @param url
     * @param username
     * @param password
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public static Connection getConnection(String driver,String url,String username,String password) throws ClassNotFoundException, SQLException {
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            oracleDateFormat(conn);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
        return conn;
    }

    public static void free(Connection conn, Statement statement, ResultSet rs) {
        if (rs != null) {
            freeResultSet(rs);
        }
        if (statement != null) {
            freeStatement(statement);
        }
        if (conn != null) {
            freeConnection(conn);
        }
    }

    /**
     * 释放数据库连接资源
     * @author zhaowj
     * @time 2015年4月10日下午1:33:22
     * @param conn
     */
    private static void freeConnection(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放数据库资源
     * @author zhaowj
     * @time 2015年4月10日下午1:33:41
     * @param statement
     */
    private static void freeStatement(Statement statement) {
        try {
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 释放数据库资源
     * @author zhaowj
     * @time 2015年4月10日下午1:33:41
     * @param rs
     */
    private static void freeResultSet(ResultSet rs) {
        try {
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
