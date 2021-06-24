package com.db;

/**
 * WeiboDB接口，可基于其实现MySQL、HDFS等多种存储服务的操作。
 */
public interface WeiboDB {
    public boolean storeText(String uid, String text, String eventName);
    public boolean storeUserInfo(String uid, String follows, String fans);
    public boolean setTable(String tableName, String rows);

}
