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
        for (MigrateTask migrateTask : migrateTaskList) {
            String zkPath=migrateTask.getZkpath()+"/"+migrateTask.getFrom()+"-"+migrateTask.getTo();
            try {
                Date lastDate=       migrateTask.getLastBinlogDate();
                long diff = (new Date().getTime() - lastDate.getTime())/1000;
                if((!migrateTask.isHaserror())&&diff>60){
                    //暂定60秒空闲 则代表增量任务结束，开始切换
                        byte[] data=   ZKUtils.getConnection().getData().forPath(zkPath) ;
                        TaskStatus taskStatus =JSON.parseObject(data, TaskStatus.class);
                        if(taskStatus.getStatus()==1)  {
                             taskStatus.setStatus(3);
                            migrateTask.setStatus(3);
                            ZKUtils.getConnection().setData().forPath(zkPath,JSON.toJSONBytes(taskStatus)) ;
                            InterProcessMutex ruleDataLock =null;
                            try {
                                String path=     ZKUtils.getZKBasePath()+"lock/ruledata.lock";
                                ruleDataLock=	 new InterProcessMutex(ZKUtils.getConnection(), path);
                                ruleDataLock.acquire(30, TimeUnit.SECONDS);
                                Map<String, SchemaConfig> schemaConfigMap= MycatServer.getInstance().getConfig().getSchemas() ;
                                SchemaConfig schemaConfig=   schemaConfigMap.get(migrateTask.getSchema());
                                TableConfig tableConfig = schemaConfig.getTables()
                                        .get(migrateTask.getTable().toUpperCase());
                                RuleConfig ruleConfig= tableConfig.getRule();
                                String ruleName=ruleConfig.getFunctionName()+"_"+ migrateTask.getTable().toUpperCase()+".properties";
                                String rulePath=ZKUtils.getZKBasePath()+"ruledata/"+ruleName;
                                    CuratorFramework zk = ZKUtils.getConnection();
                                     byte[] ruleData=zk.getData().forPath(rulePath);
                                Properties prop = new Properties();
                                prop.load(new ByteArrayInputStream(ruleData));



                            } finally {
                                try {
                                    if(ruleDataLock!=null)
                                        ruleDataLock.release();
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
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
