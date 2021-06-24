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
 * KEYIN, map阶段输入的key的类型：LongWritable
 * VALUEIN,map阶段输入value类型：Text
 * KEYOUT,map阶段输出的Key类型：Text
 * VALUEOUT,map阶段输出的value类型：IntWritable
 */
public class UserInfoMapper extends Mapper<LongWritable, Mblog, IntWritable, Text> {

    @Override
    protected void map(LongWritable var, Mblog mblog, Context context) throws IOException, InterruptedException {

        // 1 Create Table in DB
        String tableName = mblog.getEvent() + "_user";
        WeiboDB db = new SQLdb(tableName);       // Choose SQLdb

        // 2 Deliver Uid to Spider, get result
        SeleniumSpider sels = new SeleniumSpider(mblog.getUid());
        List<String> result = null;

        result = sels.run();

        // 3 Stroe results to DB
        if(result != null) {
            // Store to DB
            db.storeUserInfo(result.get(0), result.get(1), result.get(2));
        }

    }
}
