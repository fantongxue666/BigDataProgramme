package com.ftx.zkp.java_zookeeper.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 1，继承WritableComparator
 * 2，调用父类的有参构造
 * 3，指定分组的规则
 */
public class OrderGroupComparator extends WritableComparator {
    public OrderGroupComparator(){
        super(OrderBean.class,true);
    }

    //指定分组的规则（重写方法）
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //对参数做强制类型转换
        OrderBean first=(OrderBean)a;
        OrderBean second=(OrderBean)b;
        //指定分组规则
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
