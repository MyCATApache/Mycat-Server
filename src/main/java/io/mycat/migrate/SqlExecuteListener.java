package io.mycat.migrate;

import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;

import java.util.Map;

/**
 * Created by nange on 2016/12/13.
 */
public class SqlExecuteListener implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
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

        }
    }
}
