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
public class HotTopicMapper extends Mapper<Text, Text, IntWritable, Text> {

    @Override
    protected void map(Text tableName, Text url, Context context) throws IOException, InterruptedException {
        process(tableName.toString(), url.toString());

    }

    public void process(String tableName, String url) {

        // 1 Create Table in DB
        String tbName = tableName + "_mblog";
        WeiboDB db = new SQLdb(tbName);       // Choose SQLdb
        db.setTable(tbName, SQLdb.MBLOG_ROWS);

        // 2 Deliver Url to Spider, get result.
        PostsSpider ps = new PostsSpider(url, 1);
        List<Pair<String, String>> result = null;
        try {
            result = ps.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // 3 Stroe results to DB
        if(result != null) {
            // Store to DB
            for(Pair<String, String> pair : result) {
                db.storeText(pair.getKey(), modifyText(pair.getValue()), tableName);
            }
        }
    }


    // Clean data here.
    private String modifyText(String text) {
        if (text == null) return null;
        return text.replaceAll("'", "''");
    }
}
