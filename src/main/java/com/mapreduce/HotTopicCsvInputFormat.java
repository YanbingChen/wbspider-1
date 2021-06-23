package com.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class HotTopicCsvInputFormat extends FileInputFormat<Text, Text> {

    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {  //默认的记录分割的换行符，但是可以使用自定义的textinputformat.record.delimiter来替换换行符
        return new HotTopicCsvRecordReader();
    }

}

class HotTopicCsvRecordReader extends RecordReader<Text, Text>{

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
        //获取起始位置
        //起始位置(相对整个分片而言)
        long start = fileSplit.getStart();
        //获取结束位置
        //结束位置(相对整个分片而言)
        long end = start + fileSplit.getLength();
        //创建配置
        Configuration conf = context.getConfiguration();
        //获取文件路径
        Path path = fileSplit.getPath();
        //根据路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(conf);
        //打开文件输入流
        fin = fileSystem.open(path);
        //找到开始位置开始读取
        fin.seek(start);
        //创建阅读器
        reader = new LineReader(fin);

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