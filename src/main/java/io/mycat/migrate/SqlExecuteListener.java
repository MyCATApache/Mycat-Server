package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import io.mycat.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Created by nange on 2016/12/13.
 */
public class SqlExecuteListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlExecuteListener.class);
    private MigrateTask task;
   private String sql   ;
    private BinlogStream binlogStream;
    private Semaphore semaphore;
    private volatile SQLJob sqlJob;

    public SQLJob getSqlJob() {
        return sqlJob;
    }

    public void setSqlJob(SQLJob sqlJob) {
        this.sqlJob = sqlJob;
    }

    public SqlExecuteListener(MigrateTask task, String sql, BinlogStream binlogStream,Semaphore semaphore) {
        this.task = task;
        this.sql = sql;
        this.binlogStream = binlogStream;
        this.semaphore=semaphore;
    }

    @Override public void onResult(SQLQueryResult<Map<String, String>> result) {
        try {
            if (!result.isSuccess()) {
                try {
                    task.setHaserror(true);
                    pushMsgToZK(task.getZkpath(), task.getFrom() + "-" + task.getTo(), 2, "sql:" + sql + ";" + result.getErrMsg());
                    close("sucess");
                    binlogStream.disconnect();
                } catch (Exception e) {
                    LOGGER.error("error:", e);
                    close(e.getMessage());
                }
            } else {
                close("sucess");
            }

            task.setHasExecute(false);
        }finally {
            semaphore.release();
        }
    }



    private void pushMsgToZK(String rootZkPath,String child,int status,String msg) throws Exception {
        String path = rootZkPath + "/" + child;
        TaskStatus taskStatus=new TaskStatus();
        taskStatus.setMsg(msg);
        taskStatus.setStatus(status);
        task.setStatus(status);

        if(ZKUtils.getConnection().checkExists().forPath(path)==null )
        {
            ZKUtils.getConnection().create().forPath(path, JSON.toJSONBytes(taskStatus)) ;
        } else{
            ZKUtils.getConnection().setData().forPath(path, JSON.toJSONBytes(taskStatus)) ;
        }
    }
    public void close(String msg) {
        SQLJob curJob = sqlJob;
        if (curJob != null) {
            curJob.teminate(msg);
            sqlJob = null;
        }
    }
}
