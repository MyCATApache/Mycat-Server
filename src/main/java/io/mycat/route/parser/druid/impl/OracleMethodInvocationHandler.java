package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import io.mycat.route.parser.druid.SqlMethodInvocationHandler;

import java.sql.SQLNonTransientException;

/**
 * oracle函数调用
 *
 * @author zhuyiqiang
 * @version 2018/9/3
 */
public class OracleMethodInvocationHandler implements SqlMethodInvocationHandler {
    @Override
    public String invoke(SQLMethodInvokeExpr expr) throws SQLNonTransientException {
        return null;
    }
}