package io.mycat.util;


import io.mycat.MycatServer;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ZKUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);
  static   CuratorFramework curatorFramework=null;
    static {
        curatorFramework=createConnection();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override public void run() {
                   if(curatorFramework!=null)
                       curatorFramework.close();
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

        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(url, new ExponentialBackoffRetry(100, 6));

        // start connection
        curatorFramework.start();
        // wait 3 second to establish connect
        try {
            curatorFramework.blockUntilConnected(3, TimeUnit.SECONDS);
            if (curatorFramework.getZookeeperClient().isConnected()) {
                return curatorFramework;
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        // fail situation
        curatorFramework.close();
        throw new RuntimeException("failed to connect to zookeeper service : " + url);
    }

    public  static void addChildPathCache(  String path ,PathChildrenCacheListener listener )
    {
        NameableExecutor businessExecutor = MycatServer.getInstance().getBusinessExecutor();
        ExecutorService executor = businessExecutor ==null?Executors.newFixedThreadPool(5):
                businessExecutor;

        try {
            /**
             * 监听子节点的变化情况
             */
            final PathChildrenCache childrenCache = new PathChildrenCache(getConnection(), path, true);
            childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
            childrenCache.getListenable().addListener(listener,executor);
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        CuratorFramework client= ZKUtils.getConnection();
        ExecutorService executor = Executors.newFixedThreadPool(5);
       // System.out.println(client.getZookeeperClient().isConnected());
        addChildPathCache(client,ZKUtils.getZKBasePath()+"migrate",true,executor);

       Thread.sleep(8000);
    }

    private static void addChildPathCache(CuratorFramework client, final String path, final boolean addChild,  final ExecutorService executor) throws Exception {
        /**
         * 监听子节点的变化情况
         */
        final PathChildrenCache childrenCache = new PathChildrenCache(client, path, true);
        childrenCache.start(PathChildrenCache.StartMode.NORMAL);

        childrenCache.getListenable().addListener(
                new PathChildrenCacheListener() {
                    @Override
                    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                            throws Exception {
                        switch (event.getType()) {
                            case CHILD_ADDED:
                                if(addChild)
                                {

                                    addChildPathCache(client,event.getData().getPath(),false,executor);
                                }
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
                }, executor
        );

    }

}
