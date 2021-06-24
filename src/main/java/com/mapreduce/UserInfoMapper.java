package com.mapreduce;

import com.SeleniumSpider;
import com.db.SQLdb;
import com.db.WeiboDB;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;


/**
 *  CLASS: UserInfoMapper
 *  通过DB内热搜微博的条目获取用户ID，用户粉丝数和用户关注数。
 */
public class UserInfoMapper extends Mapper<LongWritable, Mblog, IntWritable, Text> {

    @Override
    protected void map(LongWritable var, Mblog mblog, Context context) throws IOException, InterruptedException {

        // 1 指定存储DB
        String tableName = mblog.getEvent() + "_user";
        WeiboDB db = new SQLdb(tableName);       // Choose SQLdb

        // 2 利用SeleniumSpider提取UID指定的用户数据
        SeleniumSpider sels = new SeleniumSpider(mblog.getUid());
        List<String> result = null;
        result = sels.run();

        // 3 存储数据到DB中
        if(result != null) {
            // Store to DB
            db.storeUserInfo(result.get(0), result.get(1), result.get(2));
        }

    }
}
