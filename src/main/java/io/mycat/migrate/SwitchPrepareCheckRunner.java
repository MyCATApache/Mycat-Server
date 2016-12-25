package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.net.NIOProcessor;
import io.mycat.route.RouteCheckRule;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by nange on 2016/12/20.
 */
public class SwitchPrepareCheckRunner implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchPrepareListener.class);
      public  static Set<String> allSwitchRunnerSet= Sets.newConcurrentHashSet();

    private String taskID;
    private String taskPath;
    private TaskNode taskNode;
    private List<PartitionByCRC32PreSlot.Range>     rangeList;

    public SwitchPrepareCheckRunner( String taskID, String taskPath,
            TaskNode taskNode,List<PartitionByCRC32PreSlot.Range>     rangeList) {
        this.taskID = taskID;
        this.taskPath = taskPath;
        this.taskNode = taskNode;
        this.rangeList=rangeList;
    }

    @Override public void run() {
        if(!allSwitchRunnerSet.contains(taskID)){
            return;
        }
        ScheduledExecutorService scheduledExecutorService= MycatServer.getInstance().getScheduler();
        ConcurrentMap<String, ConcurrentMap<String, List<PartitionByCRC32PreSlot.Range>>> migrateRuleMap = RouteCheckRule.migrateRuleMap;
        String schemal = taskNode.getSchema().toUpperCase();
        if(!migrateRuleMap.containsKey(schemal)||!migrateRuleMap.get(
                schemal).containsKey(taskNode.getTable().toUpperCase())){
           scheduledExecutorService.schedule(this,3, TimeUnit.SECONDS);
            return;
        }
       boolean isHasInTransation=false;
        NIOProcessor[] processors=MycatServer.getInstance().getProcessors();
        for (NIOProcessor processor : processors) {
            Collection<BackendConnection> backendConnections= processor.getBackends().values();
            for (BackendConnection backendConnection : backendConnections) {
                isHasInTransation=  checkIsInTransation(backendConnection);
                if(isHasInTransation){
                    scheduledExecutorService.schedule(this,3, TimeUnit.SECONDS);
                    return;
                }
            }
        }

        for (BackendConnection backendConnection : NIOProcessor.backends_old) {
            isHasInTransation=  checkIsInTransation(backendConnection);
            if(isHasInTransation){
                scheduledExecutorService.schedule(this,3, TimeUnit.SECONDS);
                return;
            }
        }

       //增加判断binlog完成
        if(!isHasInTransation){
            try {

                //先判断后端binlog都完成了才算本任务完成
               boolean allIncrentmentSucess=true;
                List<String> dataHosts=  ZKUtils.getConnection().getChildren().forPath(taskPath);
                for (String dataHostName : dataHosts) {
                    if("_prepare".equals(dataHostName)||"_commit".equals(dataHostName)||"_clean".equals(dataHostName))
                        continue;
                    List<MigrateTask> migrateTaskList= JSON
                            .parseArray(new String(ZKUtils.getConnection().getData().forPath(taskPath+"/"+dataHostName),"UTF-8") ,MigrateTask.class);
                    for (MigrateTask migrateTask : migrateTaskList) {
                        String zkPath =taskPath+"/"+dataHostName+ "/" + migrateTask.getFrom() + "-" + migrateTask.getTo();
                        if (ZKUtils.getConnection().checkExists().forPath(zkPath) != null) {
                            TaskStatus taskStatus = JSON.parseObject(
                                    new String(ZKUtils.getConnection().getData().forPath(zkPath), "UTF-8"), TaskStatus.class);
                            if (taskStatus.getStatus() != 3) {
                                allIncrentmentSucess=false;
                                break;
                            }
                        }else{
                            allIncrentmentSucess=false;
                            break;
                        }
                    }
                }
                if(allIncrentmentSucess) {
                    //需要关闭binlog，不然后续的清楚老数据会删除数据
                  BinlogStream stream=         BinlogStreamHoder.binlogStreamMap.get(taskID);
                    if(stream!=null){
                        BinlogStreamHoder.binlogStreamMap.remove(taskID);
                        stream.disconnect();
                    }

                    String myID = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);
                    String path = taskPath + "/_commit/" + myID;
                    if (ZKUtils.getConnection().checkExists().forPath(path) == null) {
                        ZKUtils.getConnection().create().creatingParentsIfNeeded().forPath(path);
                    }
                    allSwitchRunnerSet.remove(taskID);
                }   else {
                    scheduledExecutorService.schedule(this,3, TimeUnit.SECONDS);
                }
            } catch (Exception e) {
                LOGGER.error("error:",e);
            }
        }

    }




    private boolean  checkIsInTransation(BackendConnection backendConnection) {
        if(!taskNode.getSchema().equalsIgnoreCase(backendConnection.getSchema()))
            return false;

        Object attach=   backendConnection.getAttachment();
        if(attach instanceof RouteResultsetNode) {
            RouteResultsetNode resultsetNode= (RouteResultsetNode) attach;
            RouteResultset rrs= resultsetNode.getSource();
            for (String table : rrs.getTables()) {
                if(table.equalsIgnoreCase(taskNode.getTable())) {
                    int slot = resultsetNode.getSlot();
                    if(slot <0&&resultsetNode.isUpdateSql())
                    {
                       return true;

                    }  else if(resultsetNode.isUpdateSql())  {
                        for (PartitionByCRC32PreSlot.Range range : rangeList) {
                            if(slot>=range.start&&slot<=range.end){
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

}
