package com.ftx.zkp.java_zookeeper.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
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
        FileSplit fileSplit=(FileSplit) context.getInputSplit();
        String name = fileSplit.getPath().getName();
        if(name.equals("t_product.txt")){

        }else if(name.equals("t_order.txt")){

        }
        String[] split = value.toString().split(",");
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));
        System.out.println("FlowMap读取"+split[0]+"手机号信息并写入context："+flowBean.toString());
        context.write(new Text(split[0]),flowBean);
    }
}
