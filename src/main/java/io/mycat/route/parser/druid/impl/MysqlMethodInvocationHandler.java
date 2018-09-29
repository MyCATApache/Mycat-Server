package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr;
import io.mycat.route.parser.druid.SqlMethodInvocationHandler;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;

import java.sql.SQLNonTransientException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * mysql函数调用
 *
 * @author zhuyiqiang
 * @version 2018/9/3
 */
public class MysqlMethodInvocationHandler implements SqlMethodInvocationHandler {
    private final String[] SUPPORT_PATTERNS = {
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            "yyyy-MM-dd HH:mm"
    };

    @Override
    public String invoke(SQLMethodInvokeExpr expr) throws SQLNonTransientException {
        SQLExpr ret = doInvoke(expr);
        if (ret != null) {
            return ret.toString();
        }
        throw new SQLNonTransientException("unsupported mysql function expression: " + expr.toString());
    }

    private SQLExpr doInvoke(SQLMethodInvokeExpr expr) throws SQLNonTransientException {
        String methodName = expr.getMethodName().toUpperCase();
        switch (methodName) {
            case "NOW":
            case "SYSDATE":
            case "CURRENT_TIMESTAMP":
                return invokeNow();
            case "ADDDATE":
            case "DATE_ADD":
                return invokeAddDate(expr, false);
            case "SUBDATE":
            case "DATE_SUB":
                return invokeAddDate(expr, true);
        }
        return null;
    }

    private SQLExpr invokeNow() {
        String time = DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
        return new SQLCharExpr(time);
    }

    private SQLExpr invokeAddDate(SQLMethodInvokeExpr expr, boolean negative) throws SQLNonTransientException {
        List<SQLExpr> parameters = expr.getParameters();
        if (parameters.size() != 2) {
            throwSyntaxError(expr);
        }
        SQLExpr p1 = parameters.get(0);
        SQLExpr p2 = parameters.get(1);
        if (p1 instanceof SQLMethodInvokeExpr) {
            p1 = doInvoke((SQLMethodInvokeExpr) p1);
        }
        if (p1 instanceof SQLCharExpr) {
            String time = ((SQLCharExpr) p1).getText();
            Integer delta = null;
            String unit = null;
            if (p2 instanceof SQLIntegerExpr) {
                delta = (Integer) ((SQLIntegerExpr) p2).getNumber();
                unit = "DAY";
            } else if (p2 instanceof MySqlIntervalExpr) {
                SQLIntegerExpr value = (SQLIntegerExpr) ((MySqlIntervalExpr) p2).getValue();
                delta = (Integer) value.getNumber();
                unit = ((MySqlIntervalExpr) p2).getUnit().name();
            } else {
                throwSyntaxError(p2);
            }
            try {
                Date date = DateUtils.parseDate(time, SUPPORT_PATTERNS);
                Date result;
                delta = negative ? -delta : delta;
                if ("MONTH".equals(unit)) {
                    result = DateUtils.addMonths(date, delta);
                } else if ("DAY".equals(unit)) {
                    result = DateUtils.addDays(date, delta);
                } else if ("HOUR".equals(unit)) {
                    result = DateUtils.addHours(date, delta);
                } else if ("MINUTE".equals(unit)) {
                    result = DateUtils.addMinutes(date, delta);
                } else if ("SECOND".equals(unit)) {
                    result = DateUtils.addSeconds(date, delta);
                } else {
                    return null;
                }
                String ret = DateFormatUtils.format(result, "yyyy-MM-dd HH:mm:ss");
                return new SQLCharExpr(ret);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private void throwSyntaxError(SQLExpr expr) throws SQLNonTransientException {
        String errMsg = "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '" + expr + "' at line 1";
        throw new SQLNonTransientException(errMsg);
    }
}