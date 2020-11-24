package com.ftx.zkp.java_zookeeper.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName OrderBean.java
 * @Description TODO
 * @createTime 2020年11月23日 18:33:00
 */
public class OrderBean implements WritableComparable<OrderBean> {
    //实现对象序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(orderId);
    dataOutput.writeDouble(price);
    }
    //实现对象反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
    this.orderId=dataInput.readUTF();
    this.price=dataInput.readDouble();
    }
    //指定排序规则
    @Override
    public int compareTo(OrderBean orderBean) {
        //先比较订单id，如果订单id一样，则排序订单金额（降序）
        int i = this.orderId.compareTo(orderBean.getOrderId());
        if(i==0){
             i = (this.price.compareTo(orderBean.getPrice()))*(-1);
        }
        return 0;
    }

    private String orderId;
    private Double price;

    @Override
    public String toString() {
        return orderId+" "+price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

}
