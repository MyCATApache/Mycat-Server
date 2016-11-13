package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.MycatServer;
import io.mycat.util.NameableExecutor;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by magicdoom on 2016/9/28.
 */
public class MigrateTaskWatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateTaskWatch.class);
    public static void start()
    {
        CuratorFramework client= ZKUtils.getConnection();
        try {
            NameableExecutor businessExecutor = MycatServer.getInstance().getBusinessExecutor();
            ExecutorService executor = businessExecutor ==null?Executors.newFixedThreadPool(5):
                    businessExecutor;
            addChildPathCacheForTable(client,ZKUtils.getZKBasePath()+"migrate",executor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private static void addChildPathCacheForTable(CuratorFramework client, final String path, final ExecutorService executor) throws Exception {
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
                                addChildPathCacheForTask(client,event.getData().getPath(),executor);
                              LOGGER.info("table CHILD_ADDED: " + event.getData().getPath());
                                break;
                            default:
                                break;
                        }
                    }
                }, executor
        );

    }

    private static void addChildPathCacheForTask(CuratorFramework client, final String path,ExecutorService executor) throws Exception {
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
                                TaskNode taskNode=         JSON.parseObject( new String(event.getData().getData(),"UTF-8"),TaskNode.class);
                                if(!taskNode.end) {

                                }
                                System.out.println(taskNode.end);
                                LOGGER.info("table CHILD_ADDED: " + event.getData().getPath());
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
    public static void main(String[] args) throws InterruptedException {
          MigrateTaskWatch.start();
        Thread.sleep(10000);
    }
}
