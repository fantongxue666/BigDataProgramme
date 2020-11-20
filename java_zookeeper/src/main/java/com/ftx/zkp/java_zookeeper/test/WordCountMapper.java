package com.ftx.zkp.java_zookeeper.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName WordCountMapper.java
 * @Description TODO
 * @createTime 2020年11月19日 14:17:00
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    //map方法就是将K1，V1转为K2，V2

    /**
     *参数：
     * key：K1 行偏移量
     * value：V1 每一行的文本数据
     * context：表示上下文对象
     */
    /**
     * K1          V1
     * 0    hello,world,hadoop
     * 15   hdfs,hive,hello
     * --------- 转为 ----------
     * K2          V2
     * hello       1
     * world       1
     * hadoop      1
     * hdfs        1
     * hive        1
     * hello       1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text=new Text();
        LongWritable longWritable=new LongWritable();
        //将每一行文本进行拆分
        String[] worlds = value.toString().split(",");
        //遍历数组，组装K2和V2
        for(String world:worlds){
            //将K2和V2写入上下文
            text.set(world);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}
