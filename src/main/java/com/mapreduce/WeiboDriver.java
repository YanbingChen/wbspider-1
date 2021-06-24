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
    
    /**
     * Function: main
     * 分布式微博爬虫
     * arg[0] : Input file (.csv)
     * arg[1] : Output Directory - Should not exist or it will be erased.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        List<Job> jobs = new ArrayList<>();

        // STEP 1 从CSV文件读取热搜事件与URL数据
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        FSDataInputStream inStream = fs.open(path);

        // 检查CSV文件是否存在
        if(!fs.exists(path)) {
            System.out.println("File Not Exist!");
            inStream.close();
            System.exit(-1);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8));

        // 按行读入热搜事件
        String line = reader.readLine();
        while(line != null) {
            String[] split = line.split(",");
            if (split.length == 3) {
                
                // 爬取热搜URL的微博，并存入DB中的微博表(_mblog)
                HotTopicExtractor htm = new HotTopicExtractor();
                htm.process(split[0], split[1]);

                // 新建用户表(_user)
                WeiboDB db = new SQLdb(split[0]);
                db.setTable(split[0] + "_user", SQLdb.USER_ROWS);

                // 将DB内的微博条目作为输入，启动分布式计算任务
                Job j = mapreduceForUserInfo(split[0], args[1]);

                // 将任务加入列表
                if(j != null) {
                    jobs.add(j);
                }
            }

            // 读取下一行
            line = reader.readLine();
        }

        // 等待所有任务完成
        for(Job j : jobs) {
            j.waitForCompletion(true);
        }

        inStream.close();

    }

    /**
     * Function: mapreduceForUserInfo
     * Params:
     *      - String dbName: 数据源（表名）
     *      - String outFile: 输出路径（未使用）
     * 生成MapReduce Job；将数据库的表作为输入，存入对应的表中.
     */
    private static Job mapreduceForUserInfo(String dbName, String outFile) {
        try {
            // 创建配置信息
            Configuration conf = new Configuration();

            // 检查输出路径是否存在，若存在则删除
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(outFile + "/" +dbName);
            if(fs.exists(path)) {
                fs.delete(path, true);
            }

            // 输入DB相关：
            // 通过conf创建数据库配置信息
            DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.10.100:3306/wbspider?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC","wbSpider","spider");

            // 1 创建任务
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

            // 提交并返回任务对象
            job.submit();
            return job;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

}


