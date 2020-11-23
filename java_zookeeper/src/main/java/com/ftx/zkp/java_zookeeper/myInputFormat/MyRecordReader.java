package com.ftx.zkp.java_zookeeper.myInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName MyRecordReader.java
 * @Description TODO
 * @createTime 2020年11月23日 16:46:00
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    Configuration configuration = null;
    FileSplit fileSplit=null;
    private boolean processed=false;//标志文件是否读取完
    BytesWritable bytesWritable=null;
    FileSystem fileSystem =null;
    FSDataInputStream inputStream = null;
    //初始化方法
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    //获取configuration对象
         configuration = taskAttemptContext.getConfiguration();
         //获取文件的切片
        fileSplit=(FileSplit)inputSplit;

    }

    //该方法用于获取K1 和 V1

    /**
     *K1:NullWritable
     * V1:BytesWritable
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            //获取源文件的字节输入流
            //获取源文件的FileSystem
             fileSystem = FileSystem.get(configuration);
            //获取文件字节输入流
             inputStream = fileSystem.open(fileSplit.getPath());
            //读取源文件数据到普通的字节数组（byte[]）
            byte[] bytes=new byte[(int)fileSplit.getLength()];
            IOUtils.readFully(inputStream,bytes,0,(int)fileSplit.getLength());
            //把普通自己数组封装到hadoop的byteswritable中
             bytesWritable=new BytesWritable();
            bytesWritable.set(bytes,0,(int)fileSplit.getLength());
            this.processed=true;
            return true;
        }else{
            return false;
        }
        //得到V1
    }
    //是用来返回K1的
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }
    //是用来返回V1的
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }
    //获取文件读取的进度
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }
    //进行资源释放
    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }
}
