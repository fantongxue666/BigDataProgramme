package com.ftx.zkp.java_zookeeper.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName SortMapper.java
 * @Description TODO
 * @createTime 2020年11月20日 10:18:00
 */
public class SortMapper extends Mapper<LongWritable, Text,SortBean, NullWritable> {
    //map方法将K1，V1转为K2，V2

    /**
     * K1                     V1
     * 0                      a  3
     * 10                     b  7
     * ================================
     *  K2                  V2
     *  sortBean(a,3)     NullWritable
     *  sortBean(b,7)     NullWritable
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        SortBean sortBean = new SortBean();
        sortBean.setWord(split[0]);
        sortBean.setNum(Integer.parseInt(split[1]));
        context.write(sortBean,NullWritable.get());
    }
}
