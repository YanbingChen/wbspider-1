package com.db;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;


public class SQLdb implements WeiboDB{

    private String tableName;

    public static final String MBLOG_ROWS = "`id`  int NOT NULL AUTO_INCREMENT , " +
            "`uid`  varchar(255) NOT NULL , " +
            "`text`  text NOT NULL , " +
            "`event`  varchar(255) NOT NULL , " +
            "PRIMARY KEY (`id`)";

    public static final String USER_ROWS = "`id`  int NOT NULL AUTO_INCREMENT ," +
            "`uid`  varchar(255) NOT NULL ," +
            "`follows`  varchar(255) NOT NULL ," +
            "`fans`  varchar(255) NOT NULL ," +
            "PRIMARY KEY (`id`)";

    public SQLdb(String tableName) {
        this.tableName = tableName;
    }

    public boolean storeText(String uid, String text, String eventName) {
        if(tableName != null && uid != null && text != null) {
            String req = "insert into " + tableName + " (uid, text, event) values ('" + uid + "','" + text + "','" + eventName +"')";
            MysqlConnector.execute(req);
            return true;
        }
        return false;
    }

    public boolean storeUserInfo(String uid, String follows, String fans) {
        if(tableName != null && uid != null && follows != null && fans != null) {
            String req = "insert into " + tableName + " (uid, follows, fans) values ('" + uid + "','" + follows + "','" + fans +"')";
            MysqlConnector.execute(req);
            return true;
        }
        return false;
    }

    public boolean setTable(String tableName, String rows) {
        this.tableName = tableName;
        if (tableName == null) return false;

        // check table exist. If not, create it.
        String req = "CREATE TABLE if not exists `" + tableName + "` ( " +
                rows +
                " ) ENGINE=InnoDB DEFAULT CHARSET=utf8";
        MysqlConnector.execute(req);

        return true;
    }

}
