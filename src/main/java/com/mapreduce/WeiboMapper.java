package com.mapreduce;

import com.PostsSpider;
import com.SeleniumSpider;
import com.db.SQLdb;
import com.db.WeiboDB;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


/**
 * KEYIN, map阶段输入的key的类型：LongWritable
 * VALUEIN,map阶段输入value类型：Text
 * KEYOUT,map阶段输出的Key类型：Text
 * VALUEOUT,map阶段输出的value类型：IntWritable
 */
public class WeiboMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private Text outV = new Text();
    private IntWritable outK = new IntWritable(-1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1 Get Url
        String jsonUrl = value.toString();

        // 2 Deliver Url to Spider, get result.
        PostsSpider ps = new PostsSpider(jsonUrl, 1);
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
        WeiboDB db = new SQLdb();       // Choose SQLdb
        if(result != null) {
            // Store to DB
            for(Pair<String, String> pair : result) {
                db.storeText(pair.getKey(), pair.getValue());

                // Meanwhile, get uid info.
                try {
                    new SeleniumSpider(pair.getKey()).run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
