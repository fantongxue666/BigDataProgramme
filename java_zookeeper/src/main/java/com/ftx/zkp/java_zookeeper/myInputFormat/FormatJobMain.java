package com.ftx.zkp.java_zookeeper.myInputFormat;

import com.ftx.zkp.java_zookeeper.sort.SortBean;
import com.ftx.zkp.java_zookeeper.sort.SortMapper;
import com.ftx.zkp.java_zookeeper.sort.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName JobMain.java
 * @Description TODO
 * @createTime 2020年11月20日 10:25:00
 */
public class FormatJobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //创建job对象
        Job job = Job.getInstance(super.getConf(), "自定义任务名字");
        job.setInputFormatClass(MyInputFormat.class);
        Path path = new Path("file:///D:\\suibian\\mapreduce\\input.txt");
        MyInputFormat.addInputPath(job,path);
        //设置mapper类和数据类型
        job.setMapperClass(SequenceFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        //不用设置reducer类，但是一定要设置数据类型，和mapper一样，否则会报错
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        //设置输出类和输出路径
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        Path path1 = new Path("file:///D:\\suibian\\mapreduce222\\output");
        SequenceFileOutputFormat.setOutputPath(job,path1);
        //等待任务结束
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new FormatJobMain(), args);
        System.exit(run);
    }
}
