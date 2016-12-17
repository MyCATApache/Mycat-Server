package io.mycat.migrate;

import com.alibaba.fastjson.JSON;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import io.mycat.util.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by nange on 2016/12/13.
 */
public class SqlExecuteListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlExecuteListener.class);
    private MigrateTask task;
   private String sql   ;
    private BinlogStream binlogStream;

    public SqlExecuteListener(MigrateTask task, String sql, BinlogStream binlogStream) {
        this.task = task;
        this.sql = sql;
        this.binlogStream = binlogStream;
    }

    @Override public void onResult(SQLQueryResult<Map<String, String>> result) {
        if (!result.isSuccess()) {
            try {
                task.setHaserror(true);
                pushMsgToZK(task.getZkpath(),task.getFrom()+"-"+task.getTo(),2,"sql:"+sql+";"+result.getErrMsg());
            } catch (Exception e) {
              LOGGER.error("error:",e);
            }
        }
    }



    private void pushMsgToZK(String rootZkPath,String child,int status,String msg) throws Exception {
        String path = rootZkPath + "/" + child;
        TaskStatus taskStatus=new TaskStatus();
        taskStatus.setMsg(msg);
        taskStatus.setStatus(status);

        if(ZKUtils.getConnection().checkExists().forPath(path)==null )
        {
            ZKUtils.getConnection().create().forPath(path, JSON.toJSONBytes(taskStatus)) ;
        } else{
            ZKUtils.getConnection().setData().forPath(path, JSON.toJSONBytes(taskStatus)) ;
        }
    }
}
