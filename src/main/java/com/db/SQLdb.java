package com.db;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;


public class SQLdb implements WeiboDB{

    private String tableName;

    /**
     *  定义：MBLOG表
     */
    public static final String MBLOG_ROWS = "`id`  int NOT NULL AUTO_INCREMENT , " +
            "`uid`  varchar(255) NOT NULL , " +
            "`text`  text NOT NULL , " +
            "`event`  varchar(255) NOT NULL , " +
            "PRIMARY KEY (`id`)";

    /**
     *  定义：USER_ROWS表
     */
    public static final String USER_ROWS = "`id`  int NOT NULL AUTO_INCREMENT ," +
            "`uid`  varchar(255) NOT NULL ," +
            "`follows`  varchar(255) NOT NULL ," +
            "`fans`  varchar(255) NOT NULL ," +
            "PRIMARY KEY (`id`)";

    public SQLdb(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 存储mblog数据
     * @param uid 用户uid
     * @param text 微博内容
     * @param eventName 热搜事件名
     * @return TRUE表示操作成功
     */
    public boolean storeText(String uid, String text, String eventName) {
        if(tableName != null && uid != null && text != null) {
            String req = "insert into " + tableName + " (uid, text, event) values ('" + uid + "','" + text + "','" + eventName +"')";
            MysqlConnector.execute(req);
            return true;
        }
        return false;
    }

    /**
     * 存储user数据
     * @param uid 用户id
     * @param follows 关注数
     * @param fans 粉丝数
     * @return TRUE表示操作成功
     */
    public boolean storeUserInfo(String uid, String follows, String fans) {
        if(tableName != null && uid != null && follows != null && fans != null) {
            String req = "insert into " + tableName + " (uid, follows, fans) values ('" + uid + "','" + follows + "','" + fans +"')";
            MysqlConnector.execute(req);
            return true;
        }
        return false;
    }

    /**
     * 指定并建立表
     * @param tableName 表名
     * @param rows 列名与属性
     * @return True代表操作成功
     */
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
