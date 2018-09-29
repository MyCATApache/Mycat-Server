package io.mycat.route.parser.druid;

import io.mycat.route.parser.druid.impl.MysqlMethodInvocationHandler;
import io.mycat.route.parser.druid.impl.OracleMethodInvocationHandler;
import io.mycat.route.parser.druid.impl.PgsqlMethodInvocationHandler;

/**
 * 按不同的db生成对应的sql函数处理器
 *
 * @author zhuyiqiang
 * @version 2018/9/5
 */
public class SqlMethodInvocationHandlerFactory {
    private static MysqlMethodInvocationHandler mysql = new MysqlMethodInvocationHandler();
    private static OracleMethodInvocationHandler oracle = new OracleMethodInvocationHandler();
    private static PgsqlMethodInvocationHandler pgsql = new PgsqlMethodInvocationHandler();

    public static MysqlMethodInvocationHandler getForMysql() {
        return mysql;
    }

    public static OracleMethodInvocationHandler getForOracle() {
        return oracle;
    }

    public static PgsqlMethodInvocationHandler getForPgsql() {
        return pgsql;
    }
}