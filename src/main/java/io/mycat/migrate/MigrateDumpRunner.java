package io.mycat.migrate;

import com.google.common.base.Joiner;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.function.PartitionByCRC32PreSlot.Range;
import io.mycat.util.dataMigrator.DataMigratorUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by nange on 2016/12/1.
 */
public class MigrateDumpRunner implements Runnable {

    private MigrateTask task;
    private CountDownLatch latch;

    public MigrateDumpRunner(MigrateTask task, CountDownLatch latch) {
        this.task = task;
        this.latch = latch;
    }

    @Override public void run() {
        String mysqldump = "?mysqldump -h? -P? -u? -p?   ? ? --single-transaction -q --default-character-set=utf8 --hex-blob --where=\"?\" --master-data=1  -T  \"?\"  --fields-enclosed-by=\\\" --fields-terminated-by=, --lines-terminated-by=\\n ,--fields-escaped-by=\\";
        PhysicalDBPool dbPool = MycatServer.getInstance().getConfig().getDataNodes().get(task.from).getDbPool();
        PhysicalDatasource datasource = dbPool.getSources()[dbPool.getActivedIndex()];
        DBHostConfig config = datasource.getConfig();
        String filepath =
                new File(SystemConfig.getHomePath() + File.separator + "temp" , task.from + "_" + task.to
                        + Thread.currentThread().getId() + System.currentTimeMillis() + ".txt").getPath();
        String finalCmd = DataMigratorUtil
                .paramsAssignment(mysqldump,"?", "", config.getIp(), String.valueOf(config.getPort()), config.getUser(),
                config.getPassword(), task.schema, task.table, makeWhere(task), filepath);
        System.out.println(finalCmd);

        latch.countDown();
        ;
    }

    private String makeWhere(MigrateTask task) {
        List<String> whereList = new ArrayList<>();
        List<Range> slotRanges = task.slots;
        for (Range slotRange : slotRanges) {
            if (slotRange.start == slotRange.end) {
                whereList.add("_slot =" + slotRange.start);
            } else {
                whereList.add("_slot >=" + slotRange.start + " and _slot <=" + slotRange.end);
            }
        }

        return Joiner.on(" and ").join(whereList);
    }


}
