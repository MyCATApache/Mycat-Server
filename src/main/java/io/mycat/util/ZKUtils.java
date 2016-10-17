package io.mycat.util;


import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ZKUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);
  static   CuratorFramework curatorFramework=null;
    static {
        curatorFramework=createConnection();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override public void run() {
                   if(curatorFramework!=null)curatorFramework.close();
            }
        }));
    }
    public  static String getZKBasePath()
    {
       String clasterID= ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID);

        return "/mycat/"+clasterID+"/"  ;
    }
    public static CuratorFramework getConnection()
    {
        return curatorFramework;
    }

    private static CuratorFramework createConnection() {
           String url= ZkConfig.getInstance().getZkURL();

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(url, new ExponentialBackoffRetry(1000, 300));
        try {
            curatorFramework.blockUntilConnected(3,TimeUnit.SECONDS) ;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // start connection
        curatorFramework.start();
       return curatorFramework;
    }



    public static void main(String[] args) throws Exception {
        CuratorFramework client= ZKUtils.getConnection();
        System.out.println(client.getZookeeperClient().isConnected());

        /**
         * 监听子节点的变化情况
         */
        final PathChildrenCache childrenCache = new PathChildrenCache(client, ZKUtils.getZKBasePath()+"migrate", true);
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        childrenCache.getListenable().addListener(
                new PathChildrenCacheListener() {
                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                            throws Exception {
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                System.out.println("CHILD_ADDED: " + event.getData().getPath());
                                break;
                            case CHILD_REMOVED:
                                System.out.println("CHILD_REMOVED: " + event.getData().getPath());
                                break;
                            case CHILD_UPDATED:
                                System.out.println("CHILD_UPDATED: " + event.getData().getPath());
                                break;
                            default:
                                break;
                        }
                    }
                }, Executors.newCachedThreadPool()
        );

    }

}
