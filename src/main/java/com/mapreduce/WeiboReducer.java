package com.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * KEYIN, reduce阶段输入的key的类型：Text
 * VALUEIN,reduce阶段输入value类型：IntWritable
 * KEYOUT,reduce阶段输出的Key类型：Text
 * VALUEOUT,reduce阶段输出的value类型：IntWritable
 */
public class WeiboReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    private IntWritable outK = new IntWritable();
    private Text outV = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String mail = "NULL";
        String stat = "NULL";

        // 累加
        for (Text value : values) {
            String str = value.toString();
            if(str.split("@").length > 1) {
                mail = value.toString();
            } else {
                stat = value.toString();
            }
        }

        outV.set(stat + " " + mail);
        // 只写success状态的
        if (stat.equals("success"))
            // 写出
            context.write(key,outV);
    }
}
