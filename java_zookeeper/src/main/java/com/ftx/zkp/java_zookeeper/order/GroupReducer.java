package com.ftx.zkp.java_zookeeper.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName GroupReducer.java
 * @Description TODO
 * @createTime 2020年11月24日 11:08:00
 */
public class GroupReducer extends Reducer<OrderBean, Text,Text, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i=0;
        for(Text text:values){
            context.write(text,NullWritable.get());
            i++;
            if(i>=1){
                break;
            }
        }
    }
}
