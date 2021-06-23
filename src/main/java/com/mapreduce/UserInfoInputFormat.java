package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UserInfoInputFormat extends FileInputFormat<Text, Text> {

    public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        context.setStatus(genericSplit.toString());
        return new UserInfoRecordReader();
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList();
        int numLinesPerSplit = 1;
        Iterator var4 = this.listStatus(job).iterator();

        while(var4.hasNext()) {
            FileStatus status = (FileStatus)var4.next();
            splits.addAll(getSplitsForFile(status, job.getConfiguration(), numLinesPerSplit));
        }

        return splits;
    }

    public static List<FileSplit> getSplitsForFile(FileStatus status, Configuration conf, int numLinesPerSplit) throws IOException {
        List<FileSplit> splits = new ArrayList();
        Path fileName = status.getPath();
        if (status.isDirectory()) {
            throw new IOException("Not a file: " + fileName);
        } else {
            FileSystem fs = fileName.getFileSystem(conf);
            LineReader lr = null;

            try {
                FSDataInputStream in = fs.open(fileName);
                lr = new LineReader(in, conf);
                Text line = new Text();
                int numLines = 0;
                long begin = 0L;
                long length = 0L;
                boolean var14 = true;

                int num;
                while((num = lr.readLine(line)) > 0) {
                    ++numLines;
                    length += (long)num;
                    if (numLines == numLinesPerSplit) {
                        splits.add(createFileSplit(fileName, begin, length));
                        begin += length;
                        length = 0L;
                        numLines = 0;
                    }
                }

                if (numLines != 0) {
                    splits.add(createFileSplit(fileName, begin, length));
                }
            } finally {
                if (lr != null) {
                    lr.close();
                }

            }

            return splits;
        }

    }

    protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
        return begin == 0L ? new FileSplit(fileName, begin, length - 1L, new String[0]) : new FileSplit(fileName, begin - 1L, length, new String[0]);
    }

}

class UserInfoRecordReader extends RecordReader<Text, Text>{

    //文件输入流
    private FSDataInputStream fin = null;
    //key、value
    private Text key = null;
    private Text value = null;
    //定义行阅读器(hadoop.util包下的类)
    private LineReader reader = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        //获取分片
        FileSplit fileSplit = (FileSplit) split;
        //获取文件路径
        Path path = fileSplit.getPath();
        //根据路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        //打开文件输入流
        fin = fileSystem.open(path);
        //找到开始位置开始读取
        fin.seek(fileSplit.getStart());
        //创建阅读器
        reader = new LineReader(fin);

        Text line = new Text();
        if (reader.readLine(line) == 0) {
            return;
            //TODO
        }


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null){
            key = new Text();
        }
        if (value == null){
            value = new Text();
        }

        Text line = new Text();
        String[] lineParam;
        do {
            if (reader.readLine(line) == 0) {
                return false;
            }

            lineParam = line.toString().split(",");
        } while (lineParam.length < 3);

        key.set(lineParam[0]);
        value.set(lineParam[1]);

        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value ;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return 0;
    }

    @Override
    public void close() throws IOException {
        fin.close();

    }

}