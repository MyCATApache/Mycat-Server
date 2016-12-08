package io.mycat.migrate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by magicdoom on 2016/12/8.
 */
public class MigrateBinlogRunner implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrateBinlogRunner.class);
    private String dataHost;
    private List<MigrateTask> migrateTaskList;

    public MigrateBinlogRunner(String dataHost, List<MigrateTask> migrateTaskList) {
        this.dataHost = dataHost;
        this.migrateTaskList = migrateTaskList;
    }

    @Override public void run() {

    }
}
