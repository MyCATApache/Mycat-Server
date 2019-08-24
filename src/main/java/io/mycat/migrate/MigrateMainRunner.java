package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.config.model.DBHostConfig;
import io.mycat.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by magicdoom on 2016/12/8.
 */
public class MigrateMainRunner implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateMainRunner.class);
    private String dataHost;
    private List<MigrateTask> migrateTaskList;
    private int timeout;
    private Charset charset;
    private boolean forceBinlog;

    public MigrateMainRunner(String dataHost, List<MigrateTask> migrateTaskList, int timeout, Charset charset,boolean forceBinlog) {
        this.dataHost = dataHost;
        this.migrateTaskList = migrateTaskList;
        this.timeout = timeout;
        this.charset = charset;
        this.forceBinlog = forceBinlog;
    }

    @Override
    public void run() {
        try{
        AtomicInteger sucessTask = new AtomicInteger(0);
        if (!forceBinlog) {
            LOGGER.info("migrate 中 进入 mysqldump阶段");
            CountDownLatch downLatch = new CountDownLatch(migrateTaskList.size());
            for (MigrateTask migrateTask : migrateTaskList) {
                MycatServer.getInstance().getBusinessExecutor().submit(new MigrateDumpRunner(migrateTask, downLatch, sucessTask));
            }
            try {
                //modify by jian.xie 需要等到dumprunner执行结束 timeout需要改成用户指定的超时时间@cjw
                downLatch.await(this.timeout, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }else {
            LOGGER.info("migrate 中 不进入 mysqldump阶段,直接进入binlog stream");
        }
        //同一个dataHost的任务合并执行，避免过多流量浪费
        if (forceBinlog||(sucessTask.get() == migrateTaskList.size())) {
            long binlogFileNum = -1;
            String binlogFile = "";
            long pos = -1;
            for (MigrateTask migrateTask : migrateTaskList) {
                if (binlogFileNum == -1) {
                    binlogFileNum = Integer.parseInt(migrateTask.getBinlogFile().substring(migrateTask.getBinlogFile().lastIndexOf(".") + 1));
                    binlogFile = migrateTask.getBinlogFile();
                    pos = migrateTask.getPos();
                } else {
                    int tempBinlogFileNum = Integer.parseInt(migrateTask.getBinlogFile().substring(migrateTask.getBinlogFile().lastIndexOf(".") + 1));
                    if (tempBinlogFileNum <= binlogFileNum && migrateTask.getPos() <= pos) {
                        binlogFileNum = tempBinlogFileNum;
                        binlogFile = migrateTask.getBinlogFile();
                        pos = migrateTask.getPos();
                    }
                }
            }
            String taskPath = migrateTaskList.get(0).getZkpath();
            taskPath = taskPath.substring(0, taskPath.lastIndexOf("/"));
            String taskID = taskPath.substring(taskPath.lastIndexOf('/') + 1, taskPath.length());

            //开始增量数据迁移
            PhysicalDBPool dbPool = MycatServer.getInstance().getConfig().getDataHosts().get(dataHost);
            PhysicalDatasource datasource = dbPool.getSources()[dbPool.getActivedIndex()];
            DBHostConfig config = datasource.getConfig();
            BinlogStream stream = new BinlogStream(config.getUrl().substring(0, config.getUrl().indexOf(":")), config.getPort(), config.getUser(), config.getPassword(), charset);
            try {
                stream.setSlaveID(migrateTaskList.get(0).getSlaveId());
                stream.setBinglogFile(binlogFile);
                stream.setBinlogPos(pos);
                stream.setMigrateTaskList(migrateTaskList);
                BinlogStreamHoder.binlogStreamMap.put(taskID, stream);
                stream.connect();

            } catch (IOException e) {
                LOGGER.error("migrate 中 binlog 连接 异常");

                try {
                    TaskNode taskNode = JSON.parseObject(ZKUtils.getConnection().getData().forPath(taskPath), TaskNode.class);
                    taskNode.addException(e.getLocalizedMessage());
                    ZKUtils.getConnection().setData().forPath(taskPath, JSON.toJSONBytes(taskNode));
                } catch (Exception e1) {

                    LOGGER.error("error:", e);
                }
                LOGGER.error("error:", e);
            }
        }
        }catch (Exception e){
            LOGGER.error("migrate 中 binlog 连接 异常");
            e.printStackTrace();
            LOGGER.error(e.getLocalizedMessage());
        }
    }


}
