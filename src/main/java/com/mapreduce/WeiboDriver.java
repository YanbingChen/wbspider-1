package com.mapreduce;

import com.db.SQLdb;
import com.db.WeiboDB;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class WeiboDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        List<Job> jobs = new ArrayList<>();

        File f = new File(args[0]);
        if(!f.exists()) {
            System.out.println("File Not Exist!");
            System.exit(-1);
        }

        BufferedReader reader = new BufferedReader(new FileReader(f));
        String line = reader.readLine();
        while(line != null) {
            String[] split = line.split(",");
            if (split.length == 3) {
                HotTopicMapper htm = new HotTopicMapper();
                htm.process(split[0], split[1]);
                WeiboDB db = new SQLdb(split[0]);
                db.setTable(split[0] + "_user", SQLdb.USER_ROWS);

                Job j = mapreduceForDB(split[0], args[1]);
                if(j != null) {
                    jobs.add(j);
                }
            }

            line = reader.readLine();
        }

        for(Job j : jobs) {
            j.waitForCompletion(true);
        }

    }


    private static Job mapreduceForDB(String dbName, String outFile) {
        try {
            // Delete OutputFile if Exist
            File f = new File(outFile + File.pathSeparator +dbName);
            if(f.isDirectory()) {
                deleteDir(f);
            }

            //创建配置信息
            Configuration conf = new Configuration();

            // DB Related
            //通过conf创建数据库配置信息
            DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.0.51:3306/wbspider?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC","wbSpider","spider");

            //创建任务
            Job job = Job.getInstance(conf);
            job.setJobName("Extract UserInfo for db '" + dbName + "'");

            // 2 设置jar包路径
            job.setJarByClass(WeiboDriver.class);

            // 3 关联mapper和reducer
            job.setMapperClass(UserInfoMapper.class);
            job.setReducerClass(WeiboReducer.class);

            // 3.5 Set Input and Output format class
            /*job.setInputFormatClass(DBInputFormat.class);
            job.setOutputFormatClass(FileOutputFormat.class);
            job.setPartitionerClass(HashPartitioner.class);
            job.setNumReduceTasks(1);*/

            // 4 设置map输出的kv类型
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            // 5 设置最终输出的kV类型
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            // 6 设置输入路径和输出路径
            DBInputFormat.setInput(job, Mblog.class, dbName + "_mblog", null, null, new String[]{"id", "uid", "text", "event"});
            FileOutputFormat.setOutputPath(job, new Path(f.getPath()));

            job.submit();

            return job;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 递归删除目录下的所有文件及子目录下所有文件
     * @param dir 将要删除的文件目录
     * @return boolean Returns "true" if all deletions were successful.
     *                 If a deletion fails, the method stops attempting to
     *                 delete and returns "false".
     */
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}


