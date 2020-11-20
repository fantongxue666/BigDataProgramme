[TOC]

# 一，zookeeper环境搭建

ZooKeeper致力于为分布式应用提供一个高性能、高可用，且具有严格顺序访问控制能力的分布式协调服务

<table><tr style="font-weight:bold;"><td>服务器IP</td><td>主机名</td><td>myid的值</td></tr><tr><td>192.168.186.133</td><td>vmone</td><td>1</td></tr><tr><td>192.168.186.134</td><td>vmtwo</td><td>2</td></tr><tr><td>192.168.186.135</td><td>vmthree</td><td>3</td></tr></table>
myid的值越高，被选举的几率越大！

<font color="red">先把这三台机器分别重置主机名为vmone，vmtwo，vmthree，具体命令参考链接：https://blog.csdn.net/chen1092248901/article/details/81556774</font>

## 1，下载zookeeper的压缩包，下载网址如下

http://archive.apache.org/dist/zookeeper/zookeeper-3.4.10/

打开vm_one虚拟机，把zookeeper压缩包放置/export/softwares目录下准备进行安装

## 2，解压

解压zookeeper的压缩包到/export/servers路径下，然后准备进行安装

```
tar -zxvf zookeeper-3.4.10.tar.gz -C ../servers/
```

## 3，修改配置文件

第一台机器（vm_one）修改配置文件

进到 /export/servers/zookeeper-3.4.10/conf 目录，拷贝zoo_sample.cfg文件

```
cp zoo_sample.cfg zoo.cfg
vim zoo.cfg
```

```
# myid文件的存放位置
dataDir=/export/servers/zookeeper-3.4.10/zkdatas
# 保留多少个快照
autopurge.snapRetainCount=3
# 日志多少小时清除一次
autopurge.purgeInterval=1
# 集群中服务器地址（没有就新加进去）
server.1=vmone:2888:3888
server.2=vmtwo:2888:3888
server.3=vmthree:2888:3888
```

## 4，添加myid配置

在第一台机器的

/export/servers/zookeeper-3.4.10/zkdatas/这个路径下创建一个文件，文件名叫myid，文件内容为1

## 5，安装包分发并修改myid的值

安装包分发到其他机器

此处为CentOs虚拟机，克隆了两个，并修改myid分别为2和3，过程忽略

## 6，启动zookeeper

三台机器都要启动zookeeper服务，这个命令三台机器都要执行（在bin目录下）

```
./zkServer.sh start
```

验证是否启动成功  

jps需要安装openjdk   

```
yum install java-1.8.0-openjdk-devel.x86_64
```

```
[root@localhost bin]# jps
3283 Jps
2996 QuorumPeerMain    zookeeper的标志
[root@localhost bin]#
```

查看zookeeper是leader还是follower，如下命令可查看

```
./zkServer.sh status
```

zookeeper集群安装结束



# 二，zookeeper的shell客户端操作

## 1，连接zookeeper客户端

连接zookeeper客户端

```
./zkCli.sh -server vmone:2181
```

> 当启动一个zookeeper，另外两个关闭时，登录这个开启的zookeeper服务，会无限打印 Unable to read additional data from server sessionid 0x0 错误，原因是因为我在zoo.cfg中配置了3台机器，但是只启动了1台，zookeeper就会认为服务处于不可用状态，zookeeper有个选举算法，当整个集群超过半数机器宕机，zookeeper会认为集群处于不可用状态。所以，3台机器启动1台无法连接，如果启动2台及以上就可以连接了。
>
> 如果还没有解决这个问题，检查Linux是否开启了防火墙，放开端口（这里关闭了防火墙）
>
> 
>
> 另外，编辑 /etc/hosts 文件，把三台主机名和三个对应ip配置进去
>
> ![在这里插入图片描述](https://img-blog.csdnimg.cn/20201117125055193.png#pic_center)

>
>三台主机都要配置，然后就解决了这个启动问题了

```
1:查看防火状态
systemctl status firewalld
service  iptables status

2:暂时关闭防火墙
systemctl stop firewalld
service  iptables stop

3:永久关闭防火墙
systemctl disable firewalld
chkconfig iptables off

4:重启防火墙
systemctl enable firewalld
service iptables restart  
```

启动之后就可以在终端进行命令操作zookeeper了，断开连接是quit

## 2，zookeeper常用命令

命令

<table><tr><td>命令</td><td>说明</td><td>参数</td></tr><tr><td>create [-s] [-e] path data acl</td><td>创建Znode</td><td>-s 是指定顺序节点  -e 是指定临时节点</td></tr><tr><td>ls path [watch]</td><td>列出path下所有子Znode</td><td></td></tr><tr><td>get path [watch]</td><td>获取path对应的Znode的数据和属性</td><td></td></tr><tr><td>ls2 path [watch]</td><td>查看path下所有子Znode以及子Znode的属性</td><td></td></tr><tr><td>set path data [version]</td><td>更新节点</td><td>version数据版本</td></tr><tr><td>delete path [version]</td><td>删除节点</td><td>version数据版本</td></tr></table>
## 3，命令操作示例

列出path下（zookeeper的根目录下）所有Znode

```
ls /
```

创建永久节点       hello：节点名称  world  节点携带信息（携带参数）

```
create /hello world
```

创建临时节点（临时节点无法创建子节点）

```
create -e /abc world
```

创建永久序列化节点

```
create -s /zhangsan boy
```

创建临时序列化节点

```
create -e -s /lisi boy
```

修改节点数据

```
set /hello zookeeper
```

删除节点，如果要删除的节点有子节点Znode则无法删除

```
delete /hello
```

删除节点，如果有子Znode则递归删除

```
rmr /hello
```

列出历史记录

```
history
```

## 3，zookeeper的watch机制

类似于数据库的触发器，对某个Znode设置watcher，当Znode发生变化时（删除，修改，创建，子节点被修改），watchmanager就会调用对应的watcher发送给客户端。

<font color="red">watch机制只可以被触发一次，如需再次触发，只能手动再次添加！</font>

## 4，zookeeper的javaAPI操作

创建springboot工程，加入依赖

```properties
		<dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-framework</artifactId>
            <version>2.12.0</version>
        </dependency>
        <dependency>
            <groupId>org.apache.curator</groupId>
            <artifactId>curator-recipes</artifactId>
            <version>2.12.0</version>
        </dependency>
        <dependency>
            <groupId>com.google.collections</groupId>
            <artifactId>google-collections</artifactId>
            <version>1.0</version>
        </dependency>
```

### （1）创建节点

```java
	@Test
    public void test() throws Exception{
        createZnode(2);
    }

    /**
     * 创建zookeeper节点
     * param: 1：永久节点  2：临时节点
     */
    public void createZnode(Integer param) throws Exception{
        //定制一个重试策略
        /**
         * param1：重试的间隔时间
         * param2：重试的最大次数
         */
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        //获取一个客户端对象
        /**
         * param1：要连接的zookeeper服务器列表
         * param2：会话的超时时间
         * param3：链接超时时间
         * param4：重试策略
         */
        String connectionStr="192.168.186.133:2181,192.168.186.134:2181,192.168.186.135:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //开启客户端
        client.start();
        //创建节点
        if(param==1){            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello2","world".getBytes());
        }else if(param==2){            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello_tmp","world".getBytes());
            //睡眠10秒：临时节点只在会话周期内存在，如果关闭客户端，临时节点会消失，因此此处作用是为了看效果
            Thread.sleep(10000);
        }
        //关闭客户端
        client.close();
    }
```

可以配合终端查看节点 `ls /`来看节点是否创建成功

### （2）更新节点数据

```java
	/**
     * 更细节点数据
     * 节点下面添加数据和修改数据是类似的，一个节点下面会有一个数据，新的数据会覆盖掉旧的数据
     */
    public void updateZnodeData() throws Exception{
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        String connectionStr="192.168.186.133:2181,192.168.186.134:2181,192.168.186.135:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        client.start();
        client.setData().forPath("/hello2","dataValue".getBytes());
        client.close();
    }
```

### （3）查询节点数据

```java
public void getZnodeData() throws Exception{
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        String connectionStr="192.168.186.133:2181,192.168.186.134:2181,192.168.186.135:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        client.start();
        byte[] bytes = client.getData().forPath("/hello2");
        System.out.println("查询的节点数据："+bytes);
        client.close();
    }
```

### （4）节点的watch机制

```java
public void watch() throws Exception{
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        String connectionStr="192.168.186.133:2181,192.168.186.134:2181,192.168.186.135:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        client.start();
        //创建一个TreeCache对象，指定要监控的节点路径
        TreeCache treeCache=new TreeCache(client,"hello2");
        //自定义一个监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if(data!=null){ //监听器被触发
                    switch (treeCacheEvent.getType()){
                        case NODE_ADDED:
                            System.out.println("监控到有新增节点");
                            break;
                        case NODE_REMOVED:
                            System.out.println("监控到有节点被移除");
                            break;
                        case NODE_UPDATED:
                            System.out.println("监控到有节点被更新");
                            break;
                        default:
                            break;
                    }
                }
            }
        });
        //启动监听器
        treeCache.start();
        Thread.sleep(1000000);//只是为了看效果

        client.close();
    }
```

# 三 ，hadoop重新编译-准备工作

<font color="red">这里下载appach的hadoop2.7.5版本（源码版本）</font>

http://archive.apache.org/dist/hadoop/core/hadoop-2.7.5/

![在这里插入图片描述](https://img-blog.csdnimg.cn/20201117125119808.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDAwMTk2NQ==,size_16,color_FFFFFF,t_70#pic_center)


appach版本的hadoop重新编译

## 1，为什么要重新编译hadoop？

由于appach给出的hadoop安装包没有提供带C程序访问的接口，所以我们在使用本地库的时候就会出问题，需要对hadoop源码进行重新编译。

## 2，编译环境的准备

Linux环境，内存4G以上，64位操作系统

## 3，虚拟机联网，关闭防火墙，关闭selinux

```
# 关闭防火墙
systemctl stop firewalld
# 关闭selinux
vim /etc/selinux/config
设置SELINUX=disabled
```

## 4，安装jdk1.7

注意hadoop2.7.5这个版本的编译，只能使用jdk1.7，如果使用jdk1.8那么就会报错，查看是否自带openjdk，如果有则卸载掉。

> 查看是否存在自带openjdk
>
> rpm -qa|grep java
>
> 将所有openjdk全部卸载掉
>
> rpm -e --nodeps java-1.8.0-openjdk-devel-1.8.0.262.b10-0.el7_8.x86_64 java-1.7.0-openjdk-headless-1.7.0.221-2.6.18.1.el7.x86_64 java-1.8.0-openjdk-headless-1.8.0.262.b10-0.el7_8.x86_64 java-1.7.0-openjdk-1.7.0.221-2.6.18.1.el7.x86_64 tzdata-java-2020a-1.el7.noarch python-javapackages-3.4.1-11.el7.noarch javapackages-tools-3.4.1-11.el7.noarch

下载jdk1.7安装包

https://www.oracle.com/java/technologies/javase/javase7-archive-downloads.html#jdk-7u80-oth-JPR

![在这里插入图片描述](https://img-blog.csdnimg.cn/20201117125132936.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDAwMTk2NQ==,size_16,color_FFFFFF,t_70#pic_center)


下载好之后把安装包上传到/export/softwares

解压

```
cd /export/softwares
tar -zxvf jdk-7u71-linux-x64.tar.gz -C ../servers/
```

配置环境变量

```
vim /etc/profile
加入下面两句配置
export JAVA_HOME=/export/servers/jdk1.7.0_71
export PATH=:$JAVA_HOME/bin:$PATH
重新生效
source /etc/profile
```

## 5，安装maven

这里使用maven3.x版本以上都可以，强烈建议使用3.0.5版本

下载maven

http://archive.apache.org/dist/maven/maven-3/3.0.5/binaries/

![在这里插入图片描述](https://img-blog.csdnimg.cn/20201117125144900.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDAwMTk2NQ==,size_16,color_FFFFFF,t_70#pic_center)


将maven的安装包上传到/export/softwares，然后解压到/export/servers下

解压

```
tar -zxvf apache-maven-3.0.5-bin.tar.gz -C ../servers/
```

配置环境变量

```
vim /etc/profile
添加配置
export MAVEN_HOME=/export/servers/apache-maven-3.0.5
export MAVEN_OPTS="-Xms4096m -Xmx4096m"
export PATH=:$MAVEN_HOME/bin:$PATH
重新生效
source /etc/profile
查看maven版本
mvn -version
```

## 6，安装maven-repository

创建一个文件夹名为mvnrepository，用来存储maven的jar包

然后更改maven的conf文件夹中的settings.xml配置文件，指定仓库位置

```
<localRepository>/export/servers/mvnrepository</localRepository>
```

然后加入阿里云镜像

```xml
<mirror>
    <id>alimaven</id>
    <name>aliyun maven</name>
    <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
    <mirrorOf>central</mirrorOf>
</mirror>

```

## 7，安装findbugs

下载地址

https://sourceforge.net/projects/findbugs/files/findbugs/1.3.9/

![在这里插入图片描述](https://img-blog.csdnimg.cn/202011171251579.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDAwMTk2NQ==,size_16,color_FFFFFF,t_70#pic_center)


解压

```
tar -zxvf findbugs-1.3.9.tar.gz -C ../servers/
```

配置findbugs的环境变量

```
vim /etc/profile
添加配置
export FINDBUGS_HOME=/export/servers/findbugs-1.3.9
export PATH=:$FINDBUGS_HOME/bin:$PATH
重新生效
source /etc/profile
查看版本
findbugs -version
```

## 8，在线安装一些依赖包

```
yum install autoconf automake libtool cmake
yum install ncurses-devel
yum install openssl-devel
yum install lzo-devel zlib-devel gcc gcc-c++
```

bzip2压缩需要的依赖包

```
yum install -y bzip2-devel
```

## 9，安装protobuf

解压并编译

```
tar -zxvf protobuf-2.5.0.tar.gz -C ../servers/
cd protobuf-2.5.0/
./configure
make && make install
```

## 10，安装snappy

```
tar -zxvf snappy-1.1.1.tar.gz -C ../servers/
cd snappy-1.1.1
./configure
make && make install
```

## 11，编译hadoop源码

下载hadoop压缩包

下载地址：http://archive.apache.org/dist/hadoop/core/hadoop-2.7.5/

![在这里插入图片描述](https://img-blog.csdnimg.cn/20201117125208355.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80NDAwMTk2NQ==,size_16,color_FFFFFF,t_70#pic_center)


对源码进行编译

```
tar -zxvf hadoop-2.7.5-src.tar.gz -C ../servers/
cd hadoop-2.7.5-src/
```

执行下面的命令 编译支持snappy压缩

```
mvn package -DskipTests -Pdist,native -Dtar -Drequire.snappy -e -X -Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true
```

- <font color="red">必须把虚拟机的允许内存设置为4G以上，磁盘容量设置为20G，前者会报JVM崩溃错误，而后者则可能会报磁盘容量不足的错误！</font>
- <font color="red">-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true的作用：告诉maven忽略SSL证书问题，否则会报错！</font>

> 原因：由于阿里仓库地址更新成https后,下载需要使用ssl认证,如果本地没有配置的话,导致依然使用的是默认仓库，它实现maven对数据源供应商的http访问，能够使用存储在HTTP服务器中的远程存储库

编译之后，在hadoop-2.7.5-src/hadoop-dist/target目录下会有hadoop-2.7.5.tar.gz包，这也是重新编译之后的hadoop的安装包

## 12，安装hadoop

<font color="red">这里只演示单机版，以上面的vmtwo机器为例，够学习即可</font>

安装集群版参考下面链接（只是复制几份扔到另外几台机器上）

https://www.cnblogs.com/rqx-20181108/p/10278038.html

上传hadoop安装包并解压

```
tar -zxvf hadoop-2.7.5.tar.gz -C ../servers/
```

## 13，修改配置文件

因为需要修改的配置文件较多，在xshell里使用vim编辑器一个一个的改特别繁琐，这里使用nopead++工具进行远程连接虚拟机进行文件的修改。

具体连接的教程参考下方链接

使用notepad++远程编辑虚拟机文档 ： https://zhuanlan.zhihu.com/p/56313557

连接上之后找到要修改配置文件的位置，然后双击开始修改

### core-site.xml

```xml
<configuration>
    <!-- 指定集群的文件系统类型：分布式文件系统 -->
	<property>
		<name>fs.default.name</name>
		<value>hdfs://192.168.186.133:8020</value>
	</property>
    <!-- 指定临时文件存储目录 -->
	<property>
		<name>hadoop.tmp.dir</name>
		<value>/export/servers/hadoop-2.7.5/hadoopDatas/tempDatas</value>
	</property>
    <!-- 缓冲区大小 实际工作中根据服务器性能动态调整 -->
	<property>
		<name>io.file.buffer.size</name>
		<value>4096</value>
	</property>
    <!-- 开启hadoop的垃圾捅机制 删除掉的数据可以从垃圾通中回收 单位分钟 -->
	<property>
		<name>fs.trash.interval</name>
		<value>10080</value>
	</property>
</configuration>
```

### hdfs-site.xml

```xml
<configuration>
	<property>
		<name>dfs.namenode.secondary.http-address</name>
		<value>vmtwo:50090</value>
	</property>
	<!-- 指定namenode的访问地址和端口 -->
	<property>
		<name>dfs.namenode.http-address</name>
		<value>vmtwo:50070</value>
	</property>
	<!-- 指定namenode元数据的存放位置 -->
	<property>
		<name>dfs.namenode.name.dir</name>
		<value>file:///export/servers/hadoop-2.7.5/hadoopDatas/namenodeDatas,file:///export/servers/hadoop-2.7.5/hadoopDatas/namenodeDatas2</value>
	</property>
	<!-- 定义dataNode数据存储的节点位置，实际工作中，一般先确定磁盘的挂载目录，然后多个目录用，进行分割 -->
	<property>
		<name>dfs.namenode.data.dir</name>
		<value>file:///export/servers/hadoop-2.7.5/hadoopDatas/datanodeDatas,file:///export/servers/hadoop-2.7.5/hadoopDatas/datanodeDatas2</value>
	</property>
	<!-- 指定namenode的日志文件的存放目录 -->
	<property>
		<name>dfs.namenode.edits.dir</name>
		<value>file:///export/servers/hadoop-2.7.5/hadoopDatas/nn/edits</value>
	</property>
	
	<property>
		<name>dfs.namenode.checkpoint.dir</name>
		<value>file:///export/servers/hadoop-2.7.5/hadoopDatas/nn/name</value>
	</property>
	<property>
		<name>dfs.namenode.checkpoint.edits.dir</name>
		<value>file:///export/servers/hadoop-2.7.5/hadoopDatas/dfs/snn/edits</value>
	</property>
	<!-- 文本切片的复制个数 -->
	<property>
		<name>dfs.replication</name>
		<value>3</value>
	</property>
	<!-- 设置HDFS的文件权限 -->
	<property>
		<name>dfs.permissions</name>
		<value>false</value>
	</property>
	<!-- 设置一个文本切片的大小  128M-->
	<property>
		<name>dfs.blocksize</name>
		<value>134217728</value>
	</property>
</configuration>
```

### hadoop-env.sh

这个文件需要修改jdk的路径

上面重新编译hadoop时安装的是jdk1.7，这里要修改成jdk1.8，重新装一个即可

```
export JAVA_HOME=/export/servers/jdk1.8.0_231
```

### mapred-site.xml

需要把mapred-site.xml.template文件重命名为mapred-site.xml文件，然后再修改

```xml
<configuration>
	<!-- 开启mapreduce小任务模式 -->
	<property>
		<name>mapreduce.job.ubertask.enable</name>
		<value>true</value>
	</property>
	<!-- 设置历史任务的主机和端口 -->
	<property>
		<name>mapreduce.jobhistory.address</name>
		<value>vmtwo:10020</value>
	</property>
	<!-- 设置网页设置历史任务的主机和端口 -->
	<property>
		<name>mapreduce.jobhistory.webapp.address</name>
		<value>vmtwo:19888</value>
	</property>
</configuration>
```

### yarn-site.xml

```xml
<configuration>

	<!-- 配置yarn主节点的位置 -->
	<property>
		<name>yarn.resourcemanager.hostname</name>
		<value>vmtwo</value>
	</property>
	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>
	<!-- 开启日志聚合功能 -->
	<property>
		<name>yarn.log-aggregation-enable</name>
		<value>true</value>
	</property>
	<!-- 设置聚合日志在hdfs上的保存时间 -->
	<property>
		<name>yarn.log-aggregation.retain-seconds</name>
		<value>604800</value>
	</property>
	<!-- 设置yarn集群的内存分配方案 -->
	<property>
		<name>yarn.nodemanager.resource.memory-mb</name>
		<value>20480</value>
	</property>
	<property>
		<name>yarn.scheduler.minimum-allocation-mb</name>
		<value>2048</value>
	</property>
	<property>
		<name>yarn.nodemanager.vmem-pmem-ratio</name>
		<value>2.1</value>
	</property>
</configuration>
```

### mapred-env.sh

配置jdk1.8路径

```
export JAVA_HOME=/export/servers/jdk1.8.0_231
```

### slaves

写入从机名称，修改为三个主机名

```
vmtwo
```

## 14，创建文件夹

都是上面用到的文件夹，这里来创建

<font color="red">如果是集群，所有机器都要创建下面的目录</font>

```
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/tempDatas
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/namenodeDatas
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/namenodeDatas2
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/datanodeDatas
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/datanodeDatas2
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/nn/edits
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/snn/name
mkdir -p /export/server/hadoop-2.7.5/hadoopDatas/dfs/snn/edits
```

## 15，配置hadoop的环境变量

<font color="red">如果是集群，所有机器都要配置hadoop的环境变量</font>

执行以下命令

```
vim /etc/profile
```

```
export HADOOP_HOME=/export/servers/hadoop-2.7.5
export PATH=:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```

配置完生效

```
source /etc/profile
```

## 16，启动Hadoop（集群暂不做演示）

要启动hadoop，需要启动HDFS和YARN两个模块

<font color="red">注意：首次启动HDFS时，必须对其进行格式化操作，本质上是一些清理和准备工作，因为此时HDFS在物理上还是不存在的。</font>

执行以下命令

```
cd /export/servers/hadoop-2.7.5/
#  只在第一次启动时执行，看上面的注意事项 之后不可再次执行，否则会丢失文件
bin/hdfs namenode -format  
sbin/start-dfs.sh
sbin/start-yarn.sh
sbin/mr-jobhistory-daemon.sh start historyserver
```

这里因为时单机版，所以主节点和从节点都是自己！

三个端口查看页面

http://vmtwo:50070/explorer.html#/   查看hdfs

http://vmtwo:8088/cluster   查看yarn集群

http://vmtwo:19888/jobhistory   查看历史完成的任务

启动hadoop结束！

# 四，hdfs的命令行操作

## 1，基础命令

<table><tr><td>命令</td><td>作用</td><td>格式</td><td>示例</td></tr><tr><td>ls</td><td>查看文件列表</td><td>hdfs dfs -ls URI</td><td>hdfs dfs -ls /</td></tr><tr><td>lsr</td><td>递归显示某个目录下的所有文件</td><td></td><td>hdfs dfs -lsr /  旧命令
    hdfs dfs ls -R  新命令</td></tr><tr><td>mkdir</td><td>以paths中的URI作为参数，创建目录，使用 -p 参数可以递归创建目录</td><td>hdfs dfs [-p] -mkdir PATHS</td><td>hdfs dfs -mkdir /test</td></tr><tr><td>put</td><td>将单个源文件src或多个源文件srcs从本地文件系统拷贝到目标文件系统中，也可以从标准输入中读取输入，写入目标文件系统中</td><td>hdfs dfs -put LOCALSRC  ... DST</td><td>hdfs dfs -put /local/a.txt /test</td></tr><tr><td>moveFormLocal</td><td>剪切到hdfs
和put命令类似，但是源文件被删除</td><td></td><td>hdfs dfs -moveFormLocal /local/a.txt /test</td></tr><tr><td>get</td><td>将文件从hdfs拷贝到本地文件系统</td><td></td><td>hdfs dfs -get /a.txt /export/servers</td></tr><tr><td>mv</td><td>将hdfs的文件移动位置（只在hdfs里移动）</td><td></td><td>hdfs dfs -mv /dir1/a.txt  /dir2</td></tr><tr><td>rm</td><td>删除指定的文件，参数可以有多个，此命令只删除文件和非空目录，如果指定-skipTrash，则跳过回收站直接删除</td><td>hdfs dfs -rm [-r] [-skipTrash] URI [URI...]</td><td>hdfs dfs /dir/a.txt</td></tr><tr><td>cp</td><td>将文件拷贝到目标路径中 -f选项将覆盖目标，存在则覆盖 -p深度拷贝（时间戳，所有权，许可等待）</td><td>hdfs dfs -cp URI [URI...] DEST</td><td>hdfs dfs -cp /dir1/a.txt  /dir2</td></tr><tr><td>cat</td><td>将参数所指示的文件内容输出到stdout</td><td></td><td>hdfs dfs -cat /dir1/a.txt</td></tr><tr><td>chmod</td><td>改变文件权限</td><td></td><td>hdfs dfs -chmod 777 -R /dir/a.txt</td></tr><tr><td>chown</td><td>改变文件的用户和用户组，如果使用-R则目录递归执行</td><td>hdfs dfs -chown [-R] URI [URI...]</td><td>hdfs dfs -chown -R username:groupname /dir/a.txt</td></tr><tr><td>appendToFile</td><td>追加一个或多个文件到hdfs指定文件中，也可以从命令行中读取输入，前面的源路径是本地路径，后面的目标路径是hdfs文件路径</td><td>hdfs dfs -appenToFile LOCALSRC ...DST</td><td>hdfs dfs -appendToFile a.txt b.txt /big.txt</td></tr></table>

## 2，高级命令

### （1）HDFS文件限额配置

在多人共用HDFS条件下，如果没有配额管理，很容易把所有空间用完造成别人无法存取，HDFS的限额是针对目录而不是针对账号，可以让每个账号仅操作一个目录，然后对目录设置配置。

HDFS文件的限额配置允许我们以文件个数，或者文件大小来限制我们在某个目录下上传的文件数量或者文件内容总量，以便达到类似百度网盘限制每个用户允许上传的最大的文件的量。

```
hdfs dfs -count -q -h /user/root/dir  # 查看配额信息
```

*数量限额*

```
hdfs dfs -mkdir -p /user/root/dir  # 创建文件夹
hdfs dfsadmin -setQuota 2 /user/root/dir # 给文件夹下面设置最多上传两个文件，发现只能上传一个文件
```

```
hdfs dfsadmin -clrQuota /user/root/dir # 清除文件数量限制
```

*空间大小限额*

在设置空间配额时，设置的空间至少时block*3大小

```
hdfs dfsadmin -setSpaceQuota 384M /user/root/dir # 限制空间大小384M（上传文件大小）
```

生成任意大小文件的命令

```
dd if=/dev/zero of=1.txt bs=1M count=2 #生成2M的文件
```

### （2）HDFS的安全模式

安全模式是hadoop的一种保护机制，当hadoop集群启动时，会自动进入安全模式，检查数据块的完整性。

假设我们设置的副本数是3，那么在datanode中应该有3个副本，假设只存在2个副本，比例就是2/3=0.666，hdfs默认的副本率是0.999，明显小于默认副本率，因此系统会自动的复制副本到其他datanode，使得副本率不小于0.999。

> 在安全模式状态下，文件系统只接收读数据请求，而不接受删除、修改等变更请求，当整个系统达到安全标准时，HDFS会自动离开安全模式。

安全模式操作命令

```
hdfs dfsadmin -safemode get # 查看安全模式状态
hdfs dfsadmin -safemode enter # 进入安全模式
hdfs dfsadmin -safemode leave # 离开安全模式
```

### （3）HDFS的基准测试

实际生产环境中，hadoop的环境搭建完成之后，第一步就是进行压力测试，测试集群的读取和写入速度，测试我们的网络带宽是否足够等一些基准测试。

#### 测试写入速度

> 向HDFS文件系统中写入数据，10个文件，每个文件10M，文件会自动存放在/bechmarks/TestDFSIO中

```
hadoop jar hadoop2.7.5/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.5.jar TestDFSIO -write -nrFiles 10 -fileSize 10MB
```

在当前文件夹下会生成log文件，这就是读取结果报告

```
cat TestDFSIO_results.log
```

#### 测试读取速度

在HDFS文件系统中读入10个文件，每个文件10M

```
hadoop jar hadoop2.7.5/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.5.jar TestDFSIO -read -nrFiles 10 -fileSize 10MB
```

查看结果（还是上面的文件，会更新结果）

#### 清除测试数据

```
hadoop jar hadoop2.7.5/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.5.jar TestDFSIO -clean
```

# 五，hdfs的API操作

## 1，配置windows下的hadoop环境

在windows系统需要配置hadoop环境，否则直接允许代码会出现问题。

- 把windows版本的hadoop-2.7.5拷贝到一个没有中文没有空格的路径下面
- 配置hadoop环境变量
- 把hadoop-2.7.5文件夹中bin目录下的hadoop.dll文件放在系统盘：C：\windows\system32 下
- 关闭windows重启

> window版本的hadoop-2.7.5如果找不到可以使用以下方式

首先到官方下载官网的hadoop2.7.5

https://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/

然后下载hadooponwindows-master.zip

https://pan.baidu.com/s/1vxtBxJyu7HNmOhsdjLZkYw   提取码：y9a4

把hadoop-2.7.5.tar.gz解压后，使用hadooponwindows-master的bin和etc替换hadoop2.7.5的bin和etc

## 2，导入maven坐标

```xml
		<!-- hadoop开始 -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-hdfs</artifactId>
            <version>2.7.5</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-mapreduce-client-core</artifactId>
            <version>2.7.5</version>
        </dependency>
        <!-- hadoop结束 -->
```

## 3，url访问方式

实现hdfs文件下载到本地（此方法不常用）

```java
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
```

## 4，使用文件系统的方式访问数据（掌握）

> 涉及到的类：
>
> Configuration：该类的对象封装了客户端或服务器的配置
>
> FileSystem：该类的对象是一个文件系统对象，可以用该对象的一些方法来对文件进行操作，通过FileSystem的静态方法get获得该对象

```
FileSystem fs=FileSystem.get(conf)
```

### 获取FileSystem的几种方式？

第一种

```java
    public void getFileSys() throws IOException{
        //创建configuration对象
        Configuration configuration = new Configuration();
        //设置文件系统的类型
        configuration.set("fs.defaultFS","hdfs://192.168.186.133:8020");
        //获取指定的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        //输出
        System.out.println(fileSystem);
    }
```

第二种

```java
    public void getFileSys() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"),new Configuration());
        //输出
        System.out.println(fileSystem);
    }
```

第三种

```java
 public void getFileSys() throws IOException{
        //创建configuration对象
        Configuration configuration = new Configuration();
        //设置文件系统的类型
        configuration.set("fs.defaultFS","hdfs://192.168.186.133:8020");
        //获取指定的文件系统
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        //输出
        System.out.println(fileSystem.toString());
    }
```

第四种

```java
 public void getFileSys() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.186.133:8020"),new Configuration());
        //输出
        System.out.println(fileSystem);
    }
```

## 5，遍历所有文件

```java
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
```

```
hdfs://192.168.186.133:8020/tmp/a.txt----a.txt
block数：1
```

## 6，在hdfs上创建文件夹

```java
public void create() throws Exception{
        //获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"),new Configuration());
        boolean mkdirs = fileSystem.mkdirs(new Path("/dir/dir123"));//mkdirs会递归创建
        fileSystem.close();
    }
```

## 7，hdfs文件下载和上传

文件从hdfs下载到本地

```java
    public void create() throws Exception{
        //获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"),new Configuration());
       fileSystem.copyToLocalFile(new Path("/tmp/a.txt"),new Path("D:\\test.txt"));
       fileSystem.close();
    }
```

文件从本地上传到hdfs

```java
 public void create() throws Exception{
        //获取FileSystem实例
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"),new Configuration());
       fileSystem.copyFromLocalFile(new Path("file:///D:\\工作\\学习资料\\我的账号.txt"),new Path("/myAccount.txt"));
       fileSystem.close();
    }
```

## 8，hdfs文件权限控制

在上面  三（13） 处已经讲了配置文件，在那里配置了hdfs是否开启文件权限控制，如果开启了为true，则无权限操作

这里提一个知识点，在java操作文件时，并没有指定的Owner是谁，因此就算Permission有权限可还是不可以操作文件，因为Owner不对应，所以在java里可以指定Owner

```
FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"),new Configuration(),"root");
```

## 9，小文件的合并

java在上传文件时，把所有小文件合并成一个大文件然后上传

```java
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
```

# 六，MapReduce案例

<font color="red">什么是MapReduce？先上一张图，简单明了说明MapReduce的工作原理。</font>

![](https://img-blog.csdnimg.cn/20190225174340305.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl80MzAwOTg0Nw==,size_16,color_FFFFFF,t_70)

## 1，wordCount-准备工作

> 需求：在一堆给定的文本文件中统计输出每一个单词出现的总次数

数据格式准备

（1）创建一个新的文件

```
cd /export/servers
vim wordcount.txt
```

（2）向其中放入以下内容并保存

```
hello,world,hadoop
hive,sqoop,flume,hello
kitty,tom,jerry,world
hadoop
```

（3）上传到hdfs

```
hdfs dfs -mkdir /wordcount/
hdfs dfs -put wordcount.txt /wordcount/
```

## 2，Mapper

```java
package com.ftx.zkp.java_zookeeper.test;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    //map方法就是将K1，V1转为K2，V2

    /**
     *参数：
     * key：K1 行偏移量
     * value：V1 每一行的文本数据
     * context：表示上下文对象
     */
    /**
     * K1          V1
     * 0    hello,world,hadoop
     * 15   hdfs,hive,hello
     * --------- 转为 ----------
     * K2          V2
     * hello       1
     * world       1
     * hadoop      1
     * hdfs        1
     * hive        1
     * hello       1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text=new Text();
        LongWritable longWritable=new LongWritable();
        //将每一行文本进行拆分
        String[] worlds = value.toString().split(",");
        //遍历数组，组装K2和V2
        for(String world:worlds){
            //将K2和V2写入上下文
            text.set(world);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}
```

## 3,Reducer

```java
package com.ftx.zkp.java_zookeeper.test;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    //reduce作用：将新的K2，V2转成K3，V3

    /**
     *参数：
     * key：新K2
     * values：新V2
     * context：表示上下文对象
     */
    /**
     *新K2       新V2
     * hello    <1,1,1>
     * world    <1,1>
     * hadoop   <1>
     * -------转成--------
     * K3       V3
     * hello    3
     * world    2
     * hadoop   1
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        //遍历集合，将集合中的数字相加
        for(LongWritable value:values){
            count+=value.get();
        }
        //将K3，V3写入上下文中
        context.write(key,new LongWritable(count));
    }
}
```

## 4，JobMain主类

```java
package com.ftx.zkp.java_zookeeper.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
        TextInputFormat.addInputPath(job,new Path("hdfs://192.168.186.133:8020/wordcount"));
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
        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.186.133:8020/wordcount_out"));
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
```

## 5，MapReduce运行模式

### 集群运行模式

- 将MapReduce程序提交给Yarn集群，分发到很多的节点上并发执行
- 处理的数据和输出结果应该位于HDFS文件系统
- 提交集群的实现步骤：
  - 将上面写的springboot程序打成jar包
  - 上传
  - 然后在集群上用hadoop命令启动
  - 运行结束之后hdfs会在代码里指定的文件路径处生成统计结果的文件

```
hadoop jar hadoop-1.0.jar cn.ftx.mapreduce.JobMain
```

### 本地运行方式

以测试为主，MapReduce程序在本地以单进程的形式运行，处理的数据和输出结果在本地文件系统。

只需要把上面的输入路径和输出路径换成本地路径即可

```java
TextInputFormat.addInputPath(job,new Path("file:///D:\\suibian\\mapreduce"));
TextOutputFormat.setOutputPath(job,new Path("file:///D:\\suibian\\qqqqqq"));
```

<font color="red">不管在本地运行还是集群中运行，如果输出目录已经存在了，则会运行失败！</font>

解决方式：获取到路径然后判断该文件夹是否已存在，如果存在则删除即可！

```java
 		Path path = new Path("file:///D:\\suibian\\mapreduce");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.186.133:8020"), new Configuration());
        //判断目录是否存在
        boolean exists = fileSystem.exists(path);
        if(exists){
            //删除目标目录
            fileSystem.delete(path,true);// true 递归删除
        }
        TextInputFormat.addInputPath(job,path);
```

# 七，MapReduce分区

在MapReduce中，通过指定分区，会将同一个分区的数据发送给同一个reduce当中进行处理，就是有相同类型的数据，有共性的数据，送到一起去处理。

分区步骤

<font color="red">这里以彩票数据为例，txt文本存储了一条一条的中奖数据，如以下格式：空格分隔，只是例子，不要死扣下面示例代码！杠精勿扰！</font>

> 2020-10-01 12:02:56    大乐斗	16	  单		148750234
>
> 2020-10-01 11:02:20    大乐斗	12	  双		148750234
>
> 2020-10-01 10:02:38    大乐斗	25	  开		148750234
>
> 。。。

<font color="red">Mapper中的K1，V1，注意：通常K1为第一行文本的偏移量，V1为文本内容</font>

### 定义Mapper类

这个Mapper程序不做任何逻辑，也不对Key-Value做任何改变，只是接收数据，然后往下发送

```java
package com.ftx.zkp.java_zookeeper.partition;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
/**
 * K1：行偏移量
 * V1：行数据文本（必须包括要分区的值）
 *
 * K2：行数据文本
 * V2：NullWritable
 */
public class PartitionMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //map方法把K1，V1转为K2，V2
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		//把V1作为K2，V2设为null
        context.write(value,NullWritable.get());
    }
}

```

### 定义Partitioner类

主要逻辑都在这儿，通过Partitioner将数据分发给不同的Reducer

```java
package com.ftx.zkp.java_zookeeper.partition;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
public class MyPartitioner extends Partitioner<Text, NullWritable> {
    /**
     * 1，定义分区规则
     * 2，返回对应的分区编号
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        //拆分行数据文本，获取中奖字段的值
        String[] strings = text.toString().split("\t");
        String numStr=strings[2];//在行文本的第二个
        //根据15进行拆分，小于15的返回0分区编号，大于15的返回1分区编号
        if(Integer.parseInt(numStr)>15){
            return 1;
        }else {
            return 0;
        }
    }
}
```

### 定义Reducer逻辑

```java
package com.ftx.zkp.java_zookeeper.partition;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 * K2：Text
 * V2：NullWritable
 *
 * K3：Text
 * V3：NullWritable
 */
public class PartitionerReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
```

### 主类中设置分区类和ReduceTask个数

```java
package com.ftx.zkp.java_zookeeper.partition;
import com.ftx.zkp.java_zookeeper.test.WordCountMapper;
import com.ftx.zkp.java_zookeeper.test.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.net.URI;
public class PartitionJobMain extends Configured implements Tool {
    //该方法指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
        //创建一个job任务对象
        Job job = Job.getInstance(super.getConf(), "wordcount");
        //如果打包运行出错，则需要增加该配置
        job.setJarByClass(PartitionJobMain.class);
        //配置job任务对象（8个步骤）
        //1，指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
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
        job.setMapOutputValueClass(NullWritable.class);

        ######### 指定分区开始 ############
        //指定分区类
        job.setPartitionerClass(MyPartitioner.class);
        //设置ReduceTask的个数 我们这里根据15分区分了2类，所以个数是2
        job.setNumReduceTasks(2);
		######### 指定分区结束 ############

        //5，指定reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
        //6，设置K3的类型
        job.setOutputKeyClass(Text.class);
        //7，设置V3的类型
        job.setOutputValueClass(NullWritable.class);
        //8，设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //9，设置输出路径
        TextOutputFormat.setOutputPath(job,new Path("hdfs://192.168.186.133:8020/wordcount_out"));
        //等待任务结束
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new PartitionJobMain(), args);
        System.exit(run);
    }
}
```

最后把项目打成jar包上传并运行，然后结果会在hdfs文件系统生成两个结果文件，一个是小于15的列表，另一个则是大于15的列表。

# 八，MapReduce中的计数器

hadoop内置计数器有MapReduce任务计数器、文件系统计数器、FileInputFormat计数器、FileOutputFormat计数器，作业计数器。

##  1，自定义计数器

### （1）第一种方式

```java
public class PartitionMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //定义计数器  参数：计数器类型  计数器名字（描述内容）
        Counter counter = context.getCounter("MR_COUNTER", "partition_counter");
        //每次执行该方法，计数器变量值+1
        counter.increment(1L);
        
        context.write(value,NullWritable.get());
    }
}
```

### （2）第二种方式

通过enum枚举类型来定义计数器，统计reduce端数据的输入的key有多少个

```java
public class PartitionerReducer extends Reducer<Text, NullWritable,Text,NullWritable> {
    public static enum Counter{
        MY_INPUT_RECORDS,MY_INPUT_BYTES
    }
    
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //使用枚举来定义计数器
        context.getCounter(Counter.MY_INPUT_BYTES).increment(1L);
        context.write(key,NullWritable.get());
    }
}
```

# 九，MapReduce的排序和序列化

现在有一个文件有以下内容

> a		1				
>
> b		4								
>
> a		2			
>
> c		5
>
> a		9					
>
> b		6
>
> b		0
>
> 。。。

现在要对这个文件的第一列按照英文字母顺序进行排序，如果字母相同则再按照第二列的大小排序

实现效果如下

> a		1																							a		1
>
> b		4																				 	  	 a		2		
>
> a		2						>>>>>>>>>  排序后  >>>>>>>>>>	  	 a		9	
>
> c		5																					     	b		0
>
> a		9						<<<<<<<<<  排序前  <<<<<<<<<<	    	b		4
>
> b		6																				         	b		6
>
> b		0																							 c		5	
>
> 。。。																							   。。。



## 1，自定义类型和比较器

```java
public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private Integer num;

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
     @Override
    public String toString() {
        return  word + "---" + num;
    }
	//get、set方法省略
}
```

## 2，Mapper

```java
public class SortMapper extends Mapper<LongWritable, Text,SortBean, NullWritable> {
    //map方法将K1，V1转为K2，V2
    /**
     * K1                     V1
     * 0                      a  3
     * 10                     b  7
     * ================================
     *  K2                  V2
     *  sortBean(a,3)     NullWritable
     *  sortBean(b,7)     NullWritable
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        SortBean sortBean = new SortBean();
        sortBean.setWord(split[0]);
        sortBean.setNum(Integer.parseInt(split[1]));
        context.write(sortBean,NullWritable.get());
    }
}
```

## 3，Reducer

```java
public class SortReducer extends Reducer<SortBean, NullWritable,SortBean,NullWritable> {
    //reduce方法把K2，V2转为K3，V3
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
```

## 4，Main主类

```java
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //创建job对象
        Job job = Job.getInstance(super.getConf(), "mapreduce_sort");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///D:\\suibian\\mapreduce\\input.txt"));
        //设置mapper类和数据类型
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置reducer类和数据类型
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\suibian\\mapreduce222"));
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
```

本地运行，结果生成在目录D:/suibian/mapreduce222/part-r-00000中

```
a---1
a---2
a---9
b---0
b---4
b---6
c---5
```

# 十，规约Combiner

每一个map都可能产生大量的本地输出，Combiner的作用就是对map端的输出先做一次合并，以减少在map和reduce节点之间的数据传输量，以提高网络IO性能，是MapReduce优化手段之一。

<font color="red">规约操作是在map端进行的！</font>

- combiner是MR程序中mapper和reduce之间的一个组件
- zombiner组件的父类就是reducer
- combiner和reducer的区别就在于运行的位置：
  - combiner是在每一个MapTask所在的节点运行
  - reducer是接收全局所有Mapper的输出结果

- combiner的意义就是对每一个MapTask的输出进行局部汇总，以减小网络传输量

实现步骤

1. 自定义一个combiner继承reducer，重写reduce方法
2. 在job中设置job.setCombinerClass(CustomCombiner.class)

以上面的wordCount为例，加上规约

```java
public class MyCombiner extends Reducer<Text, LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count=0;
        for(LongWritable longWritable:values){
            count+=longWritable.get();
        }
        context.write(key,new LongWritable(count));
    }
}
```

> 效果描述：打一个比方，没有规约：reduce会接收map发来的100条数据，而有规约：reduce会接收map发来的30条数据，产生的结果是一样的，只是减少了数据传输量，提高了网络IO性能。

# 十一，MapReduce综合案例-统计求和步骤分析

现在有一个文件，里面是对手机访问网站的流量消耗和ip地址等信息的记录，如下

<font color="red">目的：计算汇总每个手机号（下面的列表手机号会重复）的加粗内容的之和</font>

> 13758425815	00-FD-07-A4-72-B8:CMCC	120.196.100.82	taobao.com	淘宝商城		**38	27	2481	12345**	200
> 17611117038	00-FD-07-A4-72-B8:CMCC	120.196.100.83	tianmao.com	天猫商城	**2	24	27	4950	43567**	200
> 13910567960	00-FD-07-A4-72-B8:CMCC	120.196.100.84	huya.com	虎牙直播	**4	230	27	5232	48535**	200
> 19110392563	00-FD-07-A4-72-B8:CMCC	120.196.100.00	douyu.com	斗鱼视频		**10	27	256	456**	200
>
> 。。。

## 1，自定义JavaBean存储要计算的内容

```java
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
    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+upCountFlow+"\t"+downCountFlow;
    }
   //get、set方法省略
}

```

## 2，定义FlowMapper类

```java
public class FlowMapper extends Mapper<LongWritable, Text,Text,FlowBean> {
    //map方法K1，V1转为K2，V2
    /**
     *    K1                    V1
     *    0        18892837485  。。。  98  2325    2345    234556
     *    K2                     V2
     * 18892837485       98  2325    2345    234556
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));
         //将K2和V2写入上下文
        context.write(new Text(split[1]),flowBean);
    }
}
```

## 3，定义FlowReducer类

```java
public class FlowReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //遍历集合，对集合中的对应四个字段累加
        Integer upFlow=0;
        Integer downFlow=0;
        Integer upCountFlow=0;
        Integer downCountFlow=0;
        for(FlowBean flowBean:values){
            upFlow+=flowBean.getUpFlow();
            downFlow+=flowBean.getDownFlow();
            upCountFlow+=flowBean.getUpCountFlow();
            downCountFlow+=flowBean.getDownCountFlow();
        }
        //创建对象，给对象赋值
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);
        //将K3和V3写入上下文
        context.write(key,flowBean);
    }
}
```

## 4，程序main函数

```java
public class FlowJobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //创建job对象
        Job job = Job.getInstance(super.getConf(), "mapreduce_sort");
        job.setInputFormatClass(TextInputFormat.class);
        Path path = new Path("file:///D:\\suibian\\input.txt");
        TextInputFormat.addInputPath(job,path);
        //设置mapper类和数据类型
        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        //设置reducer类和数据类型
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        Path path1 = new Path("file:///D:\\suibian\\mapreduce222\\output");
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        if(local.exists(path1)){
            local.delete(path1,true);
        }
        TextOutputFormat.setOutputPath(job,path1);
        //等待任务结束
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new FlowJobMain(), args);
        System.exit(run);
    }
}
```

