package com.mapreduce;

import com.PostsSpider;
import com.db.SQLdb;
import com.db.WeiboDB;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.IntWritable;
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
public class HotTopicExtractor {

    public static void process(String event, String url) {

        // 1 Create Table in DB
        String tableName = event + "_mblog";
        WeiboDB db = new SQLdb(tableName);       // Choose SQLdb
        db.setTable(tableName, SQLdb.MBLOG_ROWS);

        // 2 Deliver Url to Spider, get result.
        PostsSpider ps = new PostsSpider(url, 20);
        List<Pair<String, String>> result = null;
        try {
            result = ps.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3 Stroe results to DB
        if(result != null) {
            // Store to DB
            for(Pair<String, String> pair : result) {
                db.storeText(pair.getKey(), modifyText(pair.getValue()), event);
            }
        }
    }

    // Clean data here.
    private static String modifyText(String text) {
        if (text == null) return null;
        return text.replaceAll("'", "''");
    }
}
