package io.mycat.backend.heartbeat;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.sqlengine.EngineCtx;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLJobHandler;

public class HeartbeatSQLJob extends SQLJob {
    public HeartbeatSQLJob(int id, String sql, String dataNode, SQLJobHandler jobHandler, EngineCtx ctx) {
        super(id, sql, dataNode, jobHandler, ctx);
    }

    public HeartbeatSQLJob(String sql, String databaseName, SQLJobHandler jobHandler, PhysicalDatasource ds) {
        super(sql, databaseName, jobHandler, ds);
    }

    @Override
    public void okResponse(byte[] ok, BackendConnection conn) {
        // 比父类少了个conn.release(); 原因是如果这里释放了链接的话，心跳包会没有数据（responseHandler为空）
        conn.syncAndExcute();
        doFinished(false,null);
    }

}
