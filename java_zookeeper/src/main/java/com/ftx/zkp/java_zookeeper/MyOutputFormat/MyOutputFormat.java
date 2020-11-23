package com.ftx.zkp.java_zookeeper.MyOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName MyOutputFormat.java
 * @Description TODO
 * @createTime 2020年11月23日 17:33:00
 */
public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        //获取目标文件的输出流（两个）
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        //指定输出文件
        FSDataOutputStream goodCommentsOutputStream = fileSystem.create(new Path("file:///D:\\suibian\\good_out.txt"));
        FSDataOutputStream badCommentsOutputStream = fileSystem.create(new Path("file:///D:\\suibian\\bad_out.txt"));
        MyRecordWritter myRecordWritter = new MyRecordWritter(goodCommentsOutputStream, badCommentsOutputStream);
        //将输出流传给MyRecordWritter
        return myRecordWritter;
    }
}
