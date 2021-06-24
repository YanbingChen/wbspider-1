package com.mapreduce;

import com.db.SQLdb;
import com.db.WeiboDB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class WeiboDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        List<Job> jobs = new ArrayList<>();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        FSDataInputStream inStream = fs.open(path);

        if(!fs.exists(path)) {
            System.out.println("File Not Exist!");
            inStream.close();
            System.exit(-1);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8));

        String line = reader.readLine();
        while(line != null) {
            String[] split = line.split(",");
            if (split.length == 3) {
                HotTopicExtractor htm = new HotTopicExtractor();
                htm.process(split[0], split[1]);
                WeiboDB db = new SQLdb(split[0]);
                db.setTable(split[0] + "_user", SQLdb.USER_ROWS);

                Job j = mapreduceForUserInfo(split[0], args[1]);
                if(j != null) {
                    jobs.add(j);
                }
            }

            line = reader.readLine();
        }

        for(Job j : jobs) {
            j.waitForCompletion(true);
        }

        inStream.close();

    }


    private static Job mapreduceForUserInfo(String dbName, String outFile) {
        try {
            //创建配置信息
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(outFile + "/" +dbName);
            if(fs.exists(path)) {
                fs.delete(path, true);
            }

            // DB Related
            //通过conf创建数据库配置信息
            DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.10.100:3306/wbspider?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC","wbSpider","spider");

            //创建任务
            Job job = Job.getInstance(conf);
            job.setJobName("Extract UserInfo for db '" + dbName + "'");

            // 2 设置jar包路径
            job.setJarByClass(WeiboDriver.class);
            job.getJar();

            // 3 关联mapper和reducer
            job.setMapperClass(UserInfoMapper.class);
            job.setReducerClass(WeiboReducer.class);

            // 4 设置map输出的kv类型
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            // 5 设置最终输出的kV类型
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // 6 设置输入路径和输出路径
            DBInputFormat.setInput(job, Mblog.class, dbName + "_mblog", null, null, new String[]{"id", "uid", "text", "event"});
            FileOutputFormat.setOutputPath(job, path);

            job.submit();

            return job;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}


