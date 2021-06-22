package com.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


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
/*
        // 1 获取一行
        String line = value.toString();

        // 2 匹配 Starting Delivery
        String[] startDel = line.split(" starting delivery ");
        if (startDel.length > 1) {
            // Situation Starting delivery
            // 2.1 提取ID
            String[] idSplit = startDel[1].split(": ");
            outK.set(Integer.parseInt(idSplit[0]));
            String[] mailSplit = idSplit[1].split(" ");
            outV.set(mailSplit[(mailSplit.length-1)]);
            context.write(outK, outV);
            return;
        }

        // 3 匹配 Delivery
        String[] deliveryStat = line.split(" delivery ");
        if (deliveryStat.length > 1) {
            // Situation Delivery: Success/Failure/Deferral.
            // 3.1 提取ID
            String[] idSplit = deliveryStat[1].split(": ");
            outK.set(Integer.parseInt(idSplit[0]));
            outV.set(idSplit[1]);
            context.write(outK, outV);
        }
*/
    }
}
