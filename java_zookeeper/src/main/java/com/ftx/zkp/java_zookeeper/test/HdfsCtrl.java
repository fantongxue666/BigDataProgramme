package com.ftx.zkp.java_zookeeper.test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName HdfsCtrl.java
 * @Description TODO
 * @createTime 2020年11月18日 17:18:00
 */
public class HdfsCtrl {

    //实现hdfs文件下载到本地（此方法不常用）
    @Test
    public void downLoad() throws IOException {
        //注册url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //获取hdfs文件的输入流
        InputStream inputStream = new URL("hdfs://192.168.186.133:8020/tmp/a.txt").openStream();
        //获取本地文件的输出流
        FileOutputStream fileOutputStream=new FileOutputStream(new File("D:\\hello.txt"));
        IOUtils.copy(inputStream,fileOutputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileOutputStream);
    }

    @Test
    public void getFileSys() throws IOException, URISyntaxException {
        //获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"),new Configuration());
        //调用方法listFiles获取 / 目录下所有的文件信息
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator
                = fileSystem.listFiles(new Path("/"), true);//true为递归查询
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            //打印路径和文件名
            System.out.println(fileStatus.getPath()+"----"+fileStatus.getPath().getName());
            //文件的block信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("block数："+blockLocations.length);

        }
        fileSystem.close();
    }

    @Test
    public void create() throws Exception{
        //获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"),new Configuration(),"root");
        //获取hdfs大文件的输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("big_txt.txt"));
        //获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        FileStatus[] fileStatuses = local.listStatus(new Path("D:\\input"));
        //遍历每个文件，获取每个文件的输入流，并累计拷贝到hdfs新文件的输出流中
        for(FileStatus fileStatus:fileStatuses){
            FSDataInputStream open = local.open(fileStatus.getPath());
            //将小文件的数据复制到大数据
            IOUtils.copy(open,outputStream);
            IOUtils.closeQuietly(open);
        }
        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();
    }


}
