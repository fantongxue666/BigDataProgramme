package com.ftx.zkp.java_zookeeper.test;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName MyCombiner.java
 * @Description TODO
 * @createTime 2020年11月20日 12:02:00
 */
public class MyCombiner extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        for(LongWritable longWritable:values){
            count+=longWritable.get();
        }
        context.write(key,new LongWritable(count));
    }
}
