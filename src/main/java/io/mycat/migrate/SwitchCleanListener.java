package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.config.loader.zkprocess.zookeeper.ClusterInfo;
import io.mycat.route.RouteCheckRule;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**      清理本地的阻止写的规则      slaveID relese      create table
 * Ceated by magicdoom on 2016/12/19.
 */
public class SwitchCleanListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchCleanListener.class);
    @Override public void childEvent(CuratorFramework curatorFramework,
            PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED:
                 checkSwitch(event);
                break;
            default:
                break;
        }
    }
    private void checkSwitch(PathChildrenCacheEvent event)    {
        InterProcessMutex taskLock =null;
        try {

            String path=event.getData().getPath();
            String taskPath=path.substring(0,path.lastIndexOf("/_clean/"))  ;
            String taskID=taskPath.substring(taskPath.lastIndexOf('/')+1,taskPath.length());
            String lockPath=     ZKUtils.getZKBasePath()+"lock/"+taskID+".lock";
            List<String> sucessDataHost= ZKUtils.getConnection().getChildren().forPath(path.substring(0,path.lastIndexOf('/')));
            TaskNode pTaskNode= JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath),TaskNode.class);

            String custerName = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_CLUSTERID);
            ClusterInfo clusterInfo= JSON.parseObject(ZKUtils.getConnection().getData().forPath("/mycat/"+custerName) , ClusterInfo.class);
            List<String> clusterNodeList= Splitter.on(',').omitEmptyStrings().splitToList(clusterInfo.getClusterNodes());
            if(sucessDataHost.size()==clusterNodeList.size()) {

                RouteCheckRule.migrateRuleMap.remove(pTaskNode.getSchema().toUpperCase());

                List<String> needToCloseWatch=new ArrayList<>();
                List<String> dataHosts=  ZKUtils.getConnection().getChildren().forPath(taskPath);
                for (String dataHostName : dataHosts) {
                    if ("_prepare".equals(dataHostName) || "_commit".equals(dataHostName) || "_clean".equals(dataHostName))
                    {
                       needToCloseWatch.add( taskPath+"/"+dataHostName );
                    }
                }
                ZKUtils.closeWatch(needToCloseWatch);


                taskLock=	 new InterProcessMutex(ZKUtils.getConnection(), lockPath);
                taskLock.acquire(20, TimeUnit.SECONDS);
                    TaskNode taskNode= JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath),TaskNode.class);
                    if(taskNode.getStatus()==3){
                        taskNode.setStatus(5);  //clean sucess

                        //释放slaveIDs



                        for (String dataHostName : dataHosts) {
                            if("_prepare".equals(dataHostName)||"_commit".equals(dataHostName)||"_clean".equals(dataHostName))
                                continue;
                            List<MigrateTask> migrateTaskList= JSON
                                    .parseArray(new String(ZKUtils.getConnection().getData().forPath(taskPath+"/"+dataHostName),"UTF-8") ,MigrateTask.class);
                          int slaveId=  migrateTaskList.get(0).getSlaveId();
                            String slavePath=ZKUtils.getZKBasePath()+"slaveIDs/"+dataHostName+"/"+slaveId;
                            if( ZKUtils.getConnection().checkExists().forPath(slavePath)!=null) {
                                ZKUtils.getConnection().delete().forPath(slavePath);
                            }
                        }






                        ZKUtils.getConnection().setData().forPath(taskPath,JSON.toJSONBytes(taskNode))  ;
                        LOGGER.info("task end",new Date());
                    }

            }

        } catch (Exception e) {
            LOGGER.error("error:",e);
        }
        finally {
            if(taskLock!=null){
                try {
                    taskLock.release();
                } catch (Exception ignored) {

                }
            }
        }
    }






}
