package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;

import java.sql.SQLNonTransientException;

/**
 * 调用sql函数如now()等
 *
 * @author zhuyiqiang
 * @version 2018/9/3
 */
public interface SqlMethodInvocationHandler {
    /**
     * 调用sql函数，返回结果
     * @param expr 函数表达式
     * @return 执行结果
     * @throws SQLNonTransientException
     */
    String invoke(SQLMethodInvokeExpr expr) throws SQLNonTransientException;
}