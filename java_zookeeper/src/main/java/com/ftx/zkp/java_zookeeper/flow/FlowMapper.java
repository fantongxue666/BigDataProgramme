package com.ftx.zkp.java_zookeeper.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName FlowMapper.java
 * @Description TODO
 * @createTime 2020年11月20日 14:30:00
 */
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    //map方法K1，V1转为K2，V2
    /**
     *    K1                    V1
     *    0        18892837485  。。。  98  2325    2345    234556
     *    K2                     V2
     * 18892837485       98  2325    2345    234556
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[7]));
        flowBean.setDownFlow(Integer.parseInt(split[8]));
        flowBean.setUpCountFlow(Integer.parseInt(split[9]));
        flowBean.setDownCountFlow(Integer.parseInt(split[10]));
        context.write(new Text(split[1]),flowBean);
    }
}