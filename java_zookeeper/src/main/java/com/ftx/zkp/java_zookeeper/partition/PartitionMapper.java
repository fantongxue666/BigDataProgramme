package com.ftx.zkp.java_zookeeper.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName PartitionMapper.java
 * @Description TODO
 * @createTime 2020年11月19日 17:00:00
 */

/**
 * K1：行偏移量
 * V1：行数据文本（必须包括要分区的值）
 *
 * K2：行数据文本
 * V2：NullWritable
 */
public class PartitionMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //map方法把K1，V1转为K2，V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //定义计数器  参数：计数器类型  计数器名字（描述内容）
        Counter counter = context.getCounter("MR_COUNTER", "partition_counter");
        //每次执行该方法，计数器变量值+1
        counter.increment(1L);

        context.write(value,NullWritable.get());
    }
}
