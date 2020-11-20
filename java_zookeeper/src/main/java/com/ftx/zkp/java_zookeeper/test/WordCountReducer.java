package com.ftx.zkp.java_zookeeper.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName WordCountReducer.java
 * @Description TODO
 * @createTime 2020年11月19日 14:32:00
 */
public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    //reduce作用：将新的K2，V2转成K3，V3

    /**
     *参数：
     * key：新K2
     * values：新V2
     * context：表示上下文对象
     */
    /**
     *新K2       新V2
     * hello    <1,1,1>
     * world    <1,1>
     * hadoop   <1>
     * -------转成--------
     * K3       V3
     * hello    3
     * world    2
     * hadoop   1
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        //遍历集合，将集合中的数字相加
        for(LongWritable value:values){
            count+=value.get();
        }
        //将K3，V3写入上下文中
        context.write(key,new LongWritable(count));
    }
}
