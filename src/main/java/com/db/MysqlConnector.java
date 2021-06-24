package com.db;

import java.sql.*;

public class MysqlConnector {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://192.168.10.100:3306/wbspider?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    static final String USER = "wbSpider";  // INSERT CREATE SELECT quanxian
    static final String PASS = "spider";

    private static Connection conn;

    static boolean isAvailable = false;

    public static void init() {
        try {
            // Class.forName(JDBC_DRIVER);

            // Open Connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            isAvailable = conn != null;
        }
    }

    public synchronized static ResultSet request(String req) {
        if(conn == null) init();
        ResultSet rs = null;
        try {
            Statement stmt = conn.createStatement();
            rs = stmt.executeQuery(req);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return rs;
    }

    public synchronized static void execute(String req) {
        if(conn == null) init();
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(req);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public synchronized static DatabaseMetaData getMetaData() throws Exception {
        if(conn == null) init();
        //获取数据库的元数据
        return conn.getMetaData();
    }
}
