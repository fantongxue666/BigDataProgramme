package com.ftx.zkp.java_zookeeper.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName SortReducer.java
 * @Description TODO
 * @createTime 2020年11月20日 10:22:00
 */
public class SortReducer extends Reducer<SortBean, NullWritable,SortBean,NullWritable> {
    //reduce方法把K2，V2转为K3，V3
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
