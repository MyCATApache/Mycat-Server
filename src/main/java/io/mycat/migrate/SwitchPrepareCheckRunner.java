package io.mycat.migrate;

import com.google.common.collect.Sets;
import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.net.NIOProcessor;
import io.mycat.route.RouteCheckRule;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.function.PartitionByCRC32PreSlot;

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
        ScheduledExecutorService scheduledExecutorService= MycatServer.getInstance().getScheduler();
        ConcurrentMap<String, ConcurrentMap<String, List<PartitionByCRC32PreSlot.Range>>> migrateRuleMap = RouteCheckRule.migrateRuleMap;
        String schemal = taskNode.getSchema().toUpperCase();
        if(!migrateRuleMap.containsKey(schemal)||migrateRuleMap.get(
                schemal).containsKey(taskNode.getTable().toUpperCase())){
           scheduledExecutorService.schedule(this,3, TimeUnit.SECONDS);
        }
       boolean isHasInTransation=false;
        NIOProcessor[] processors=MycatServer.getInstance().getProcessors();
        for (NIOProcessor processor : processors) {
            Collection<BackendConnection> backendConnections= processor.getBackends().values();
            for (BackendConnection backendConnection : backendConnections) {
                isHasInTransation=  checkIsInTransation(backendConnection);
                if(isHasInTransation){
                    scheduledExecutorService.schedule(this,3, TimeUnit.SECONDS);
                }
            }
        }

        for (BackendConnection backendConnection : NIOProcessor.backends_old) {
            isHasInTransation=  checkIsInTransation(backendConnection);
            if(isHasInTransation){
                scheduledExecutorService.schedule(this,3, TimeUnit.SECONDS);
            }
        }

        //todo
        if(!isHasInTransation){

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
