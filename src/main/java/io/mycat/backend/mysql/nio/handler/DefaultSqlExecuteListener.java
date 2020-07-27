package io.mycat.backend.mysql.nio.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.server.ServerConnection;

/**
 * sql各自执行阶段（读取，解析，路由，执行，完成）
 * @author funny 2020年7月26日 下午9:36:59
 * @since 1.0.0
 */
public class DefaultSqlExecuteListener implements SqlExecuteListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSqlExecuteListener.class);

    private ServerConnection source;

    public DefaultSqlExecuteListener(ServerConnection source) {
        this.source = source;
    }

    @Override
    public void fireEvent(SQL_EXECUTE_STAGE stage) {
        switch (stage) {
        case READ:
            onReadCompleted();
            break;
        case PARSE:
            onParseCompleted();
            break;
        case ROUTE:
            onRouteCompleted();
            break;
        case EXECUTE:
            onExecuteCompleted();
            break;
        case MERGE:
            onMergeCompleted();
            break;
        case END:
            onEndCompleted();
            break;

        }
    }

    private void onReadCompleted() {

    }

    private void onParseCompleted() {

    }

    private void onRouteCompleted() {

    }

    private void onExecuteCompleted() {

    }

    private void onMergeCompleted() {

    }

    private void onEndCompleted() {
        LOGGER.debug("on event sql end");
        source.onEventSqlCompleted();
    }

}

