package com.ftx.zkp.java_zookeeper.myInputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName MyInputFormat.java
 * @Description TODO
 * @createTime 2020年11月23日 16:44:00
 */
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {


    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //创建自定义RecordReader对象
        MyRecordReader myRecordReader = new MyRecordReader();
        //将inputSplit和TaskAttemptContext传给myRecordReader
        myRecordReader.initialize(inputSplit,taskAttemptContext);
        return myRecordReader;
    }

    //设置文件是否可以切割
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;//这里不需要
    }
}
