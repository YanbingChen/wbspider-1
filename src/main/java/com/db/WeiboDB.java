package com.db;

public interface WeiboDB {
    public boolean storeText(String uid, String text);
    public boolean stroeUserInfo(String uid, String follows, String fans);

}
