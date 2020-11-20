package com.ftx.zkp.java_zookeeper.test;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * @author FanJiangFeng
 * @version 1.0.0
 * @ClassName TestCtrl.java
 * @Description TODO
 * @createTime 2020年11月14日 17:34:00
 */
public class TestCtrl {

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
        if(param==1){
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello2","world".getBytes());
        }else if(param==2){
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/hello_tmp","world".getBytes());
            //睡眠10秒：临时节点只在会话周期内存在，如果关闭客户端，临时节点会消失，因此此处作用是为了看效果
            Thread.sleep(10000);
        }
        //关闭客户端
        client.close();
    }

    /**
     * 更细节点数据
     * 节点下面添加数据和修改数据是类似的，一个节点下面会有一个数据，新的数据会覆盖掉旧的数据
     */
    public void updateZnodeData() throws Exception{
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,1);
        String connectionStr="192.168.186.133:2181,192.168.186.134:2181,192.168.186.135:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        client.start();
//        client.setData().forPath("/hello2","dataValue".getBytes());
        byte[] bytes = client.getData().forPath("/hello2");
        System.out.println("查询的节点数据："+bytes);
        client.close();
    }

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



}
