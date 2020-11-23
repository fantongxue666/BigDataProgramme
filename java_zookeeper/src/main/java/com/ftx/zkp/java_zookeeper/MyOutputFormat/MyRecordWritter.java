package com.ftx.zkp.java_zookeeper.MyOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName MyRecordWritter.java
 * @Description TODO
 * @createTime 2020年11月23日 17:33:00
 */
public class MyRecordWritter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream goodCommentsOutputStream;
    private FSDataOutputStream badCommentsOutputStream;
    public MyRecordWritter(){}

    public MyRecordWritter(FSDataOutputStream goodCommentsOutputStream, FSDataOutputStream badCommentsOutputStream) {
        this.goodCommentsOutputStream = goodCommentsOutputStream;
        this.badCommentsOutputStream = badCommentsOutputStream;
    }

    //行文本内容
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        //从行文本中获取评论值
        String[] split = text.toString().split("\t");
        String numStr=split[9];
        //根据字段值判断评论类型然后将对应的数据写入不同的文件夹文件中
        if(Integer.parseInt(numStr)<=1){//好评+中评
        goodCommentsOutputStream.write(text.toString().getBytes());
        goodCommentsOutputStream.write("\r\n".getBytes());
        }else{//差评
        badCommentsOutputStream.write(text.toString().getBytes());
        badCommentsOutputStream.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        goodCommentsOutputStream.close();
        badCommentsOutputStream.close();
    }
}
