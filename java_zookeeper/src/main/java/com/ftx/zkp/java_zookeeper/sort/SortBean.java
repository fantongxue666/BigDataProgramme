package com.ftx.zkp.java_zookeeper.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName SortBean.java
 * @Description TODO
 * @createTime 2020年11月20日 10:05:00
 */
public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private Integer num;

    @Override
    public String toString() {
        return  word + "---" + num;
    }
    //实现比较器，指定排序规则

    /**
     *规则：第一列按照字典顺序排序，如果字母相同，第二列再按照大小排序
     */
    @Override
    public int compareTo(SortBean sortBean) {
        //先对第一列排序，如果第一列相同，再按照第二列排序
        int i = this.word.compareTo(sortBean.getWord());
        if(i==0){//相同
            return this.num.compareTo(sortBean.getNum());
        }
        return i;//大于0则前者比后者大，小于0则反之
    }

    //实现序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }
    //实现反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.num=dataInput.readInt();
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}
