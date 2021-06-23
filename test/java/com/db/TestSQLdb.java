package com.db;

public class TestSQLdb {

    public static void main(String[] args) {
        MysqlConnector.init();

        WeiboDB db = new SQLdb();
        db.storeText("12134156", "Hello, World!");
        db.stroeUserInfo("12134156", "22", "33");
    }


}
