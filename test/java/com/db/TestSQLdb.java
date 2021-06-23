package com.db;

public class TestSQLdb {

    public static void main(String[] args) {
        MysqlConnector.init();

        WeiboDB db = new SQLdb("mytable");
        db.setTable("mytable", SQLdb.MBLOG_ROWS);
        db.storeText("12134156", "Hello, World!", "mytable");
    }


}
