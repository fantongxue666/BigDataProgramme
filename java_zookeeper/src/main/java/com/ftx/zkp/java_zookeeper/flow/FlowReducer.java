package com.ftx.zkp.java_zookeeper.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName FlowReducer.java
 * @Description TODO
 * @createTime 2020年11月20日 14:35:00
 */
public class FlowReducer extends Reducer<Text,FlowBean, Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //遍历集合，对集合中的对应四个字段累加
        Integer upFlow=0;
        Integer downFlow=0;
        Integer upCountFlow=0;
        Integer downCountFlow=0;
        for(FlowBean flowBean:values){
            upFlow+=flowBean.getUpFlow();
            downFlow+=flowBean.getDownFlow();
            upCountFlow+=flowBean.getUpCountFlow();
            downCountFlow+=flowBean.getDownCountFlow();
        }
        //创建对象，给对象赋值
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        //将K3和V3写入上下文
        System.out.println("FlowReducer读取"+key.toString()+"手机号信息并写入context："+flowBean.toString());
        context.write(key,flowBean);
    }
}
