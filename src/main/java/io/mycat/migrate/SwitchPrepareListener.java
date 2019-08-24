package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.route.RouteCheckRule;
import io.mycat.route.function.PartitionByCRC32PreSlot;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by magicdoom on 2016/12/19.
 */
public class SwitchPrepareListener implements PathChildrenCacheListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(SwitchPrepareListener.class);

    @Override
    public void childEvent(CuratorFramework curatorFramework,
                           PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED:
                checkSwitch(event);
                break;
            default:
                break;
        }
    }

    private void checkSwitch(PathChildrenCacheEvent event) {
        InterProcessMutex taskLock = null;
        try {

            String path = event.getData().getPath();
            String taskPath = path.substring(0, path.lastIndexOf("/_prepare/"));
            String taskID = taskPath.substring(taskPath.lastIndexOf('/') + 1, taskPath.length());
            String lockPath = ZKUtils.getZKBasePath() + "lock/" + taskID + ".lock";
            List<String> sucessDataHost = ZKUtils.getConnection().getChildren().forPath(path.substring(0, path.lastIndexOf('/')));
            List<MigrateTask> allTaskList = MigrateUtils.queryAllTask(taskPath, sucessDataHost);
            TaskNode pTaskNode = JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath), TaskNode.class);

            ConcurrentMap<String, List<PartitionByCRC32PreSlot.Range>> tableRuleMap =
                    RouteCheckRule.migrateRuleMap.containsKey(pTaskNode.getSchema().toUpperCase()) ?
                            RouteCheckRule.migrateRuleMap.get(pTaskNode.getSchema().toUpperCase()) :
                            new ConcurrentHashMap();
            tableRuleMap.put(pTaskNode.getTable().toUpperCase(), MigrateUtils.convertAllTask(allTaskList));
            RouteCheckRule.migrateRuleMap.put(pTaskNode.getSchema().toUpperCase(), tableRuleMap);


            taskLock = new InterProcessMutex(ZKUtils.getConnection(), lockPath);
            taskLock.acquire(20, TimeUnit.SECONDS);

            List<String> dataHost = ZKUtils.getConnection().getChildren().forPath(taskPath);
            if (getRealSize(dataHost) == sucessDataHost.size()) {
                TaskNode taskNode = JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath), TaskNode.class);
                if (taskNode.getStatus() == 1) {
                    taskNode.setStatus(2);  //prepare switch
                    LOGGER.info("task switch:", new Date());
                    ZKUtils.getConnection().setData().forPath(taskPath, JSON.toJSONBytes(taskNode));
                }
            }
        } catch (Exception e) {
            LOGGER.error("error:", e);
        } finally {
            if (taskLock != null) {
                try {
                    taskLock.release();
                } catch (Exception ignored) {

                }
            }
        }
    }

    private int getRealSize(List<String> dataHosts) {
        int size = dataHosts.size();
        Set<String> set = new HashSet(dataHosts);
        if (set.contains("_prepare")) {
            size = size - 1;
        }
        if (set.contains("_commit")) {
            size = size - 1;
        }
        if (set.contains("_clean")) {
            size = size - 1;
        }
        return size;
    }


}
