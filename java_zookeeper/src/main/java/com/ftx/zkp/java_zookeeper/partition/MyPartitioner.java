package com.ftx.zkp.java_zookeeper.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName MyPartitioner.java
 * @Description TODO
 * @createTime 2020年11月19日 17:06:00
 */
public class MyPartitioner extends Partitioner<Text, NullWritable> {
    /**
     * 1，定义分区规则
     * 2，返回对应的分区编号
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        //拆分行数据文本，获取中奖字段的值
        String[] strings = text.toString().split("\t");
        String numStr=strings[5];//在行文本的第六个
        //根据15进行拆分，小于15的返回0分区编号，大于15的返回1分区编号
        if(Integer.parseInt(numStr)>15){
            return 1;
        }else {
            return 0;
        }

    }
}
