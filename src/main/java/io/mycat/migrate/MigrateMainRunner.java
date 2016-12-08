package io.mycat.migrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by magicdoom on 2016/12/8.
 */
public class MigrateMainRunner implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateMainRunner.class);
    private String dataHost;
    private List<MigrateTask> migrateTaskList;

    public MigrateMainRunner(String dataHost, List<MigrateTask> migrateTaskList) {
        this.dataHost = dataHost;
        this.migrateTaskList = migrateTaskList;
    }

    @Override public void run() {
        AtomicInteger sucessTask=new AtomicInteger(0);
        for (MigrateTask migrateTask : migrateTaskList) {
            new MigrateDumpRunner(migrateTask,sucessTask).run();
        }
         //同一个dataHost的任务合并执行，避免过多流量浪费
        if(sucessTask.get()==migrateTaskList.size())
        {
             int binlogFileNum=-1;
             int pos=-1;
            for (MigrateTask migrateTask : migrateTaskList) {
                if(binlogFileNum==-1){
                    binlogFileNum=Integer.parseInt(migrateTask.getBinlogFile().substring(migrateTask.getBinlogFile().lastIndexOf(".")+1)) ;
                    pos=migrateTask.getPos();
                }  else{
                   int tempBinlogFileNum=Integer.parseInt(migrateTask.getBinlogFile().substring(migrateTask.getBinlogFile().lastIndexOf(".")+1)) ;
                    if(tempBinlogFileNum<=binlogFileNum&&migrateTask.getPos()<=pos) {
                       binlogFileNum=tempBinlogFileNum;
                        pos=migrateTask.getPos();
                    }
                }
            }

           //开始增量数据迁移
            System.out.println();

        }
    }
}
