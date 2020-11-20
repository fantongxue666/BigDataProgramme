package com.ftx.zkp.java_zookeeper.flow;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName FlowBean.java
 * @Description TODO
 * @createTime 2020年11月20日 14:23:00
 */
public class FlowBean implements Writable {
    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(upFlow);
    dataOutput.writeInt(downFlow);
    dataOutput.writeInt(upCountFlow);
    dataOutput.writeInt(downCountFlow);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow=dataInput.readInt();
        this.downFlow=dataInput.readInt();
        this.upCountFlow=dataInput.readInt();
        this.downCountFlow=dataInput.readInt();
    }

    private Integer upFlow;//上行流量
    private Integer downFlow;//下行流量
    private Integer upCountFlow;//上行流量总和
    private Integer downCountFlow;//下行流量总和


    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }
    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+upCountFlow+"\t"+downCountFlow;
    }
}
