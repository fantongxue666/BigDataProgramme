package com.ftx.zkp.java_zookeeper.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName JobMain.java
 * @Description TODO
 * @createTime 2020年11月19日 15:17:00
 */
public class JobMain extends Configured implements Tool {
    //该方法指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), "wordcount");
        //如果打包运行出错，则需要增加该配置
        job.setJarByClass(JobMain.class);
        //配置job任务对象（8个步骤）
        //1，指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job,new Path("hdfs://192.168.186.133:8020/wordcount"));
        Path path = new Path("file:///D:\\suibian\\mapreduce");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"), new Configuration());
        //判断目录是否存在
        boolean exists = fileSystem.exists(path);
        if(exists){
            //删除目标目录
            fileSystem.delete(path,true);// true 递归删除
        }
        TextInputFormat.addInputPath(job,path);
        //2，指定map阶段的处理方式和数据类型
        job.setMapperClass(WordCountMapper.class);
        //3，设置map阶段K2的类型
        job.setMapOutputKeyClass(Text.class);
        //4，设置map阶段V2的类型
        job.setMapOutputValueClass(LongWritable.class);
        //5，指定reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
        //6，设置K3的类型
        job.setOutputKeyClass(Text.class);
        //7，设置V3的类型
        job.setOutputValueClass(LongWritable.class);
        //8，设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //9，设置输出路径
//        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.186.133:8020/wordcount_out"));
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\suibian\\qqqqqq"));
        //等待任务结束
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
