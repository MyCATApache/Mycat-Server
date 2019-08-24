package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

/**
 * Created by magicdoom on 2016/12/14.
 */
public class BinlogIdleCheck implements Runnable {
    private BinlogStream binlogStream;
    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogIdleCheck.class);
    public BinlogIdleCheck(BinlogStream binlogStream) {
        this.binlogStream = binlogStream;
    }

    @Override public void run() {
        List<MigrateTask>migrateTaskList= binlogStream.getMigrateTaskList();
        int sucessSwitchTask=0;
        int fullSucessSwitchTask=0;
        String taskPath=null;
        String dataHost=null;
        for (MigrateTask migrateTask : migrateTaskList) {
            String zkPath=migrateTask.getZkpath();
            if(taskPath==null){
                taskPath=zkPath.substring(0,zkPath.lastIndexOf("/")) ;
                dataHost=zkPath.substring(zkPath.lastIndexOf("/")+1);
            }
            if(migrateTask.isHaserror()||migrateTask.isHasExecute())
            {
                continue;
            }
                Date lastDate=       migrateTask.getLastBinlogDate();
                long diff = (new Date().getTime() - lastDate.getTime())/1000;
                if((!migrateTask.isHaserror())&&diff>30){
                    //暂定30秒空闲 则代表增量任务结束，开始切换
                   sucessSwitchTask=sucessSwitchTask+1;
                    fullSucessSwitchTask=fullSucessSwitchTask+1;

                }else if(!migrateTask.isHaserror()){
                    String sql=MigrateUtils.makeCountSql(migrateTask);
                    try {
                        long oldCount=MigrateUtils.execulteCount(sql,migrateTask.getFrom());
                        long newCount=MigrateUtils.execulteCount(sql,migrateTask.getTo());
                        if(oldCount!=0) {
                            double percent = newCount / oldCount;
                            if(percent>=0.9) {
                                sucessSwitchTask=sucessSwitchTask+1;
                            }
                        }
                    } catch (SQLException e) {
                        LOGGER.error("error:",e);
                    } catch (IOException e) {
                        LOGGER.error("error:",e);
                    }

                }

        }


        try {
        	byte[] taskNodeStr = ZKUtils.getConnection().getData().forPath(taskPath);
        	System.out.println(new String(taskNodeStr));
            TaskNode taskNode = JSON.parseObject(taskNodeStr,TaskNode.class);
            if(taskNode.getStatus()>=3){
                    binlogStream.disconnect();
            }
        } catch (Exception e) {
            LOGGER.error("error:",e);
        }


        if(sucessSwitchTask==migrateTaskList.size()){
           String  childTaskPath=taskPath+"/_prepare/"+dataHost;
            try {
                if( ZKUtils.getConnection().checkExists().forPath(childTaskPath)==null) {
                   ZKUtils.getConnection().create().creatingParentsIfNeeded().forPath(childTaskPath);
                }

            } catch (Exception e) {
                LOGGER.error("error:",e);
            }

        }


          //全部空闲后，如果已经开始切换了，则修改每个子任务状态
        if(fullSucessSwitchTask==migrateTaskList.size()){
            try {
                TaskNode taskNode=JSON.parseObject(new String( ZKUtils.getConnection().getData().forPath(taskPath),"UTF-8"),TaskNode.class);
                 if(taskNode.getStatus()==2) {

                     for (MigrateTask migrateTask : migrateTaskList) {
                         String zkPath = migrateTask.getZkpath() + "/" + migrateTask.getFrom() + "-" + migrateTask.getTo();
                         if (ZKUtils.getConnection().checkExists().forPath(zkPath) != null) {
                             TaskStatus taskStatus = JSON.parseObject(
                                     new String(ZKUtils.getConnection().getData().forPath(zkPath), "UTF-8"), TaskStatus.class);
                             if (taskStatus.getStatus() == 1) {
                                 taskStatus.setStatus(3);
                                 ZKUtils.getConnection().setData().forPath(zkPath, JSON.toJSONBytes(taskStatus));
                             }
                         }
                     }
                 }
            } catch (Exception e) {
                LOGGER.error("error:",e);
            }

        }
    }





}
