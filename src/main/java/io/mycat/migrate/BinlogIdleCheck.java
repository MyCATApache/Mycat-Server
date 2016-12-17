package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                            ZKUtils.getConnection().setData().forPath(zkPath,JSON.toJSONBytes(taskStatus)) ;
                        }

                }

            } catch (Exception e) {
                LOGGER.error("error:",e);
            }
        }
    }


}
