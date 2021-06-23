package com.db;

import org.apache.commons.lang3.tuple.Pair;

public class SQLdb implements WeiboDB{

    public boolean storeText(String uid, String text) {
        if(uid != null && text != null) {
            text = text.replaceAll("'", "''");
            String req = "insert into mblog_info (uid, text) values ('" + uid + "','" + text +"')";
            MysqlConnector.execute(req);
            return true;
        }
        return false;
    }

    public boolean stroeUserInfo(String uid, String follows, String fans) {
        if(uid != null && follows != null && fans != null) {
            String req = "insert into user_info (uid, follows, fans) values ('" + uid + "','" + follows + "','" + fans +"')";
            MysqlConnector.execute(req);
            return true;
        }
        return false;
    }

}
