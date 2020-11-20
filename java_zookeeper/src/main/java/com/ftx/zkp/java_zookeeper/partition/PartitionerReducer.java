package com.ftx.zkp.java_zookeeper.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName PartitionerReducer.java
 * @Description TODO
 * @createTime 2020年11月19日 17:16:00
 */

/**
 * K2：Text
 * V2：NullWritable
 *
 * K3：Text
 * V3：NullWritable
 */
public class PartitionerReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    public static enum Counter{
        MY_INPUT_RECORDS,MY_INPUT_BYTES
    }
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //使用枚举来定义计数器
        context.getCounter(Counter.MY_INPUT_BYTES).increment(1L);
        context.write(key,NullWritable.get());
    }
}
