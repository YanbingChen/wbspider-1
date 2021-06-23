package com.db;

public interface WeiboDB {
    public boolean storeText(String uid, String text, String eventName);
    public boolean storeUserInfo(String uid, String follows, String fans);
    public boolean setTable(String tableName, String rows);

}
