package com.ftx.zkp.java_zookeeper.myInputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName SequenceFileMapper.java
 * @Description TODO
 * @createTime 2020年11月23日 17:09:00
 */
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        //获取文件名字，作为K2
        FileSplit fileSplit=(FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();
        //K2和V2写入上下文
        context.write(new Text(name),value);
    }
}
