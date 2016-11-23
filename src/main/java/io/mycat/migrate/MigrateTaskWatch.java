package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.model.SchemaConfig;
import io.mycat.util.NameableExecutor;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ......./migrate/schemal/tableName/taskid/dn   [任务数据]
 * Created by magicdoom on 2016/9/28.
 */
public class MigrateTaskWatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateTaskWatch.class);
    public static void start()
    {
            ZKUtils.addChildPathCache(ZKUtils.getZKBasePath() + "migrate", new PathChildrenCacheListener() {
                @Override public void childEvent(CuratorFramework curatorFramework,
                        PathChildrenCacheEvent fevent) throws Exception {

                    switch (fevent.getType()) {
                        case CHILD_ADDED:
                            LOGGER.info("table CHILD_ADDED: " + fevent.getData().getPath());
                            ZKUtils.addChildPathCache(fevent.getData().getPath(), new PathChildrenCacheListener() {
                                @Override public void childEvent(CuratorFramework curatorFramework,
                                        PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                                    switch (pathChildrenCacheEvent.getType()) {
                                        case CHILD_ADDED:
                                            ZKUtils.addChildPathCache(pathChildrenCacheEvent.getData().getPath(),new TaskPathChildrenCacheListener()) ;
                                            break;
                                    }
                                }
                            });
                            break;
                        default:
                            break;
                    }
                }
            });

    }


    private static Set<String> getDataNodeFromDataHost(List<String> dataHosts)
    {
        Set<String> dataHostSet= Sets.newConcurrentHashSet(dataHosts) ;
        Set<String> dataNodes = new HashSet<>();
        Map<String, PhysicalDBNode> dataNodesMap= MycatServer.getInstance().getConfig().getDataNodes();
        for (Map.Entry<String, PhysicalDBNode> stringPhysicalDBNodeEntry : dataNodesMap.entrySet()) {
            String key=stringPhysicalDBNodeEntry.getKey();
            PhysicalDBNode value=stringPhysicalDBNodeEntry.getValue();
           String dataHostName= value.getDbPool().getHostName();
            if(dataHostSet.contains(dataHostName)){
                dataNodes.add(key);
            }
        }


        return dataNodes;
    }



    public static void main(String[] args) throws InterruptedException {
          MigrateTaskWatch.start();
      //  Thread.sleep(10000);
    }

    private static class TaskPathChildrenCacheListener implements PathChildrenCacheListener {
        @Override public void childEvent(CuratorFramework curatorFramework,
                PathChildrenCacheEvent event) throws Exception {
            switch (event.getType()) {
                case CHILD_ADDED:
                    addOrUpdate(event);
                    LOGGER.info("table CHILD_ADDED: " + event.getData().getPath());
                    break;
                case CHILD_UPDATED:
                    addOrUpdate(event);
                    LOGGER.info("CHILD_UPDATED: " + event.getData().getPath());
                    break;
                default:
                    break;
            }
        }

        private void addOrUpdate(PathChildrenCacheEvent event) throws Exception {
            String text = new String(event.getData().getData(), "UTF-8");

            List<String> dataNodeList= ZKUtils.getConnection().getChildren().forPath(event.getData().getPath());
            if(!dataNodeList.isEmpty())    {
            TaskNode taskNode=         JSON.parseObject(
                    text,TaskNode.class);
            if(!taskNode.end) {
             String boosterDataHosts=   ZkConfig.getInstance().getValue(ZkParamCfg.MYCAT_BOOSTER_DATAHOSTS) ;
                Set<String> dataNodes=getDataNodeFromDataHost(Splitter.on(",").trimResults().omitEmptyStrings().splitToList(boosterDataHosts)) ;
                List<MigrateTask> finalMigrateList=new ArrayList<>();
                for (String s : dataNodeList) {
                    if(dataNodes.contains(s)) {
                      String data=new String(ZKUtils.getConnection().getData().forPath(event.getData().getPath() + "/" + s),"UTF-8");
                        List<MigrateTask> migrateTaskList= JSONArray.parseArray(data,MigrateTask.class);
                        finalMigrateList.addAll(migrateTaskList);
                    }
                }

                System.out.println(finalMigrateList.size());
            }  }
        }
    }
}
