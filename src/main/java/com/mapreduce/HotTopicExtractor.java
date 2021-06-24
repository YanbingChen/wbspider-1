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
 *  CLASS: HotTopicExtractor
 *  METHODS:
 *      - static void process(String, String): 通过URL提取热搜，并存入数据库
 */
public class HotTopicExtractor {

    public static void process(String event, String url) {

        // 1 在DB内创建Table
        String tableName = event + "_mblog";
        WeiboDB db = new SQLdb(tableName);       // Choose SQLdb
        db.setTable(tableName, SQLdb.MBLOG_ROWS);

        // 2 将参数传入PostSpider并获得结果
        PostsSpider ps = new PostsSpider(url, 20);
        List<Pair<String, String>> result = null;
        try {
            result = ps.run();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3 将结果存入DB对应的表中
        if(result != null) {
            // Store to DB
            for(Pair<String, String> pair : result) {
                db.storeText(pair.getKey(), modifyText(pair.getValue()), event);
            }
        }
    }

    // 针对Text数据的清洗与转译
    private static String modifyText(String text) {
        if (text == null) return null;
        return text.replaceAll("'", "''");
    }
}
