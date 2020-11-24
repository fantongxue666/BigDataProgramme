package com.ftx.zkp.java_zookeeper.order;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName OrderPartition.java
 * @Description TODO
 * @createTime 2020年11月24日 10:51:00
 */
public class OrderPartition extends Partitioner<OrderBean, Text> {
    //分区规则：根据订单的id实现分区

    /**
     *
     * @param orderBean
     * @param text
     * @param i ReduceTask个数
     * @return  返回分区的编号
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        return (orderBean.getOrderId().hashCode() & 2147483647) % i;
    }
}
