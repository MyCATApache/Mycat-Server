package io.mycat.backend.mysql.listener;

/**
 * 定义sql各个执行阶段（读取，解析，路由，执行，完成）监听事件，比如写日志，写执行统计
 * @author funnyAnt 2020年7月26日 下午9:54:51
 * @since 1.0.0
 */
public interface SqlExecuteStageListener {
    void fireEvent(SqlExecuteStage stage);

}

