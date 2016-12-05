package io.mycat.migrate;

import com.alibaba.druid.util.JdbcUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.function.PartitionByCRC32PreSlot.Range;
import io.mycat.util.ProcessUtil;
import io.mycat.util.dataMigrator.DataMigratorUtil;
import io.mycat.util.dataMigrator.DataNode;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static io.mycat.util.dataMigrator.DataMigratorUtil.executeQuery;
import static io.mycat.util.dataMigrator.DataMigratorUtil.getMysqlConnection;

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
        String mysqldump = "?mysqldump -h? -P? -u? -p?  ? ? --single-transaction -q --default-character-set=utf8 --hex-blob --where=\"?\" --master-data=1  -T  \"?\"  --fields-enclosed-by=\\\" --fields-terminated-by=, --lines-terminated-by=\\n  --fields-escaped-by=\\ ";
        PhysicalDBPool dbPool = MycatServer.getInstance().getConfig().getDataNodes().get(task.from).getDbPool();
        PhysicalDatasource datasource = dbPool.getSources()[dbPool.getActivedIndex()];
        DBHostConfig config = datasource.getConfig();
        File file = null;
       String spath=   querySecurePath(config);
        if(Strings.isNullOrEmpty(spath)||"NULL".equalsIgnoreCase(spath)||"empty".equalsIgnoreCase(spath)) {
            file = new File(SystemConfig.getHomePath() + File.separator + "temp",
                    task.from + "_" + task.to + Thread.currentThread().getId() + System.currentTimeMillis() + "");
        }   else {
            spath+= Thread.currentThread().getId() + System.currentTimeMillis();
            file=new File(spath);
        }
        file.mkdirs();

        String finalCmd = DataMigratorUtil
                .paramsAssignment(mysqldump,"?", "", config.getIp(), String.valueOf(config.getPort()), config.getUser(),
                config.getPassword(), task.schema, task.table, makeWhere(task), file.getPath());
      String result=  ProcessUtil.execReturnString(finalCmd);
        int logIndex = result.indexOf("MASTER_LOG_FILE='");
        int logPosIndex = result.indexOf("MASTER_LOG_POS=");
        String logFile=result.substring(logIndex +17,logIndex +17+result.substring(logIndex +17).indexOf("'")) ;
        String logPos=result.substring(logPosIndex +15,logPosIndex +15+result.substring(logPosIndex +15).indexOf(";")) ;
        try {
          String xxx=  Files.toString(new File(file,task.table+".txt"), Charset.forName("UTF-8")) ;
            System.out.println(xxx);
        } catch (IOException e) {
            e.printStackTrace();
        }
        latch.countDown();

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

    private static String querySecurePath(DBHostConfig config  )  {
        List<Map<String, Object>> list=null;
        String path = null;
        Connection con = null;
        try {
            con =  DriverManager.getConnection("jdbc:mysql://"+config.getUrl(),config.getUser(),config.getPassword());
            list = executeQuery(con, "show variables like 'secure_file_priv'");
            if(list!=null&&list.size()==1)
            path = (String) list.get(0).get("Value");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }finally{
            JdbcUtils.close(con);
        }
        return path;
    }

    public static void main(String[] args) {
        String result="\n" + "--\n" + "-- Position to start replication or point-in-time recovery from\n" + "--\n"
                + "\n" + "CHANGE MASTER TO MASTER_LOG_FILE='NANGE-PC-bin.000021', MASTER_LOG_POS=154;\n";
        int logIndex = result.indexOf("MASTER_LOG_FILE='");
        int logPosIndex = result.indexOf("MASTER_LOG_POS=");
        String logFile=result.substring(logIndex +17,logIndex +17+result.substring(logIndex +17).indexOf("'")) ;
        String logPos=result.substring(logPosIndex +15,logPosIndex +15+result.substring(logPosIndex +15).indexOf(";")) ;
        System.out.println();
    }
}
