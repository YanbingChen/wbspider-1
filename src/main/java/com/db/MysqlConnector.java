package com.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MysqlConnector {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://192.168.0.51:3306/wbspider?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    static final String USER = "wbSpider";
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
}
