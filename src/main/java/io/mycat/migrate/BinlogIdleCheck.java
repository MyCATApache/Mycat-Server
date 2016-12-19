package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import com.google.common.io.Files;
import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
        String taskPath=null;
        String dataHost=null;
        for (MigrateTask migrateTask : migrateTaskList) {
            String zkPath=migrateTask.getZkpath();
            if(taskPath==null){
                taskPath=zkPath.substring(0,zkPath.lastIndexOf("/")) ;
                dataHost=zkPath.substring(zkPath.lastIndexOf("/")+1);
            }
                Date lastDate=       migrateTask.getLastBinlogDate();
                long diff = (new Date().getTime() - lastDate.getTime())/1000;
                if((!migrateTask.isHaserror())&&diff>60){
                    //暂定60秒空闲 则代表增量任务结束，开始切换
                   sucessSwitchTask=sucessSwitchTask+1;

                }

        }
        if(sucessSwitchTask==migrateTaskList.size()){
             taskPath=taskPath+"/_status/"+dataHost;
            try {
                if( ZKUtils.getConnection().checkExists().forPath(taskPath)==null) {
                   ZKUtils.getConnection().create().creatingParentsIfNeeded().forPath(taskPath);
                }

            } catch (Exception e) {
                LOGGER.error("error:",e);
            }

        }
    }



    private void xx()
    {
//        byte[] data=   ZKUtils.getConnection().getData().forPath(zkPath) ;
//        TaskStatus taskStatus =JSON.parseObject(data, TaskStatus.class);
//        if(taskStatus.getStatus()==1)  {
//            taskStatus.setStatus(3);
//            migrateTask.setStatus(3);
//            ZKUtils.getConnection().setData().forPath(zkPath,JSON.toJSONBytes(taskStatus)) ;
//            InterProcessMutex ruleDataLock =null;
//            try {
//                String path=     ZKUtils.getZKBasePath()+"lock/ruledata.lock";
//                ruleDataLock=	 new InterProcessMutex(ZKUtils.getConnection(), path);
//                ruleDataLock.acquire(30, TimeUnit.SECONDS);
//                Map<String, SchemaConfig> schemaConfigMap= MycatServer.getInstance().getConfig().getSchemas() ;
//                SchemaConfig schemaConfig=   schemaConfigMap.get(migrateTask.getSchema());
//                TableConfig tableConfig = schemaConfig.getTables()
//                        .get(migrateTask.getTable().toUpperCase());
//                RuleConfig ruleConfig= tableConfig.getRule();
//                String ruleName=ruleConfig.getFunctionName()+"_"+ migrateTask.getTable().toUpperCase()+".properties";
//                String rulePath=ZKUtils.getZKBasePath()+"ruledata/"+ruleName;
//                CuratorFramework zk = ZKUtils.getConnection();
//                byte[] ruleData=zk.getData().forPath(rulePath);
//                Properties prop = new Properties();
//                prop.load(new ByteArrayInputStream(ruleData));
//                modifyRuleData(prop,migrateTask,tableConfig);
//                ByteArrayOutputStream out=new ByteArrayOutputStream();
//                prop.store(out, "WARNING   !!!Please do not modify or delete this file!!!");
//                zk.setData().forPath(ruleName, out.toByteArray());
//
//            } finally {
//                try {
//                    if(ruleDataLock!=null)
//                        ruleDataLock.release();
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
    }

    private   void modifyRuleData( Properties prop ,MigrateTask task, TableConfig tableConfig ){
        int fromIndex=-1;
        int toIndex=-1;
    List<String> dataNodes=   tableConfig.getDataNodes();
        for (int i = 0; i < dataNodes.size(); i++) {
            String dataNode = dataNodes.get(i);
            if(dataNode.equalsIgnoreCase(task.getFrom())){
                fromIndex=i;
            } else
            if(dataNode.equalsIgnoreCase(task.getTo())){
                toIndex=i;
            }
        }
      String from=  prop.getProperty(String.valueOf(fromIndex)) ;
    }


}
