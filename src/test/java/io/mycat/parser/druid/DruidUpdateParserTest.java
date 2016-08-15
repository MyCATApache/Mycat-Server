package io.mycat.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.impl.DruidUpdateParser;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/7
 */

public class DruidUpdateParserTest {
    /**
     * 测试单表更新分片字段
     * @throws NoSuchMethodException
     */
    @Test
    public void testUpdateShardColumn() throws NoSuchMethodException{
        throwExceptionParse("update hotnews set id = 1 where name = 234;", true);
        throwExceptionParse("update hotnews set id = 1 where id = 3;", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and name = '234'", false);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 or name = '234'", true);
        throwExceptionParse("update hotnews set id = 'A', name = '123' where id = 'A' and name = '234'", false);
        throwExceptionParse("update hotnews set id = 'A', name = '123' where id = 'A' or name = '234'", true);
        throwExceptionParse("update hotnews set id = 1.5, name = '123' where id = 1.5 and name = '234'", false);
        throwExceptionParse("update hotnews set id = 1.5, name = '123' where id = 1.5 or name = '234'", true);

        throwExceptionParse("update hotnews set id = 1, name = '123' where name = '234' and (id = 1 or age > 3)", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and (name = '234' or age > 3)", false);

        // 子查询，特殊的运算符between等情况
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and name in (select name from test)", false);
        throwExceptionParse("update hotnews set id = 1, name = '123' where name = '123' and id in (select id from test)", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id between 1 and 3", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id between 1 and 3 and name = '234'", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id between 1 and 3 or name = '234'", true);
        throwExceptionParse("update hotnews set id = 1, name = '123' where id = 1 and name between '124' and '234'", false);
    }

    /**
     * 测试单表别名更新分片字段
     * @throws NoSuchMethodException
     */
    @Test
    public void testAliasUpdateShardColumn() throws NoSuchMethodException{
        throwExceptionParse("update hotnews h set h.id = 1 where h.name = 234;", true);
        throwExceptionParse("update hotnews h set h.id = 1 where h.id = 3;", true);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.id = 1 and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.id = 1 or h.name = '234'", true);
        throwExceptionParse("update hotnews h set h.id = 'A', h.name = '123' where h.id = 'A' and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 'A', h.name = '123' where h.id = 'A' or h.name = '234'", true);
        throwExceptionParse("update hotnews h set h.id = 1.5, h.name = '123' where h.id = 1.5 and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 1.5, h.name = '123' where h.id = 1.5 or h.name = '234'", true);

        throwExceptionParse("update hotnews h set id = 1, h.name = '123' where h.id = 1 and h.name = '234'", false);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where id = 1 or h.name = '234'", true);

        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.name = '234' and (h.id = 1 or h.age > 3)", true);
        throwExceptionParse("update hotnews h set h.id = 1, h.name = '123' where h.id = 1 and (h.name = '234' or h.age > 3)", false);
    }

    public void throwExceptionParse(String sql, boolean throwException) throws NoSuchMethodException {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement sqlStatement = statementList.get(0);
        MySqlUpdateStatement update = (MySqlUpdateStatement) sqlStatement;
        SchemaConfig schemaConfig = mock(SchemaConfig.class);
        Map<String, TableConfig> tables = mock(Map.class);
        TableConfig tableConfig = mock(TableConfig.class);
        String tableName = "hotnews";
        when((schemaConfig).getTables()).thenReturn(tables);
        when(tables.get(tableName)).thenReturn(tableConfig);
        when(tableConfig.getParentTC()).thenReturn(null);
        RouteResultset routeResultset = new RouteResultset(sql, 11);
        Class c = DruidUpdateParser.class;
        Method method = c.getDeclaredMethod("confirmShardColumnNotUpdated", new Class[]{SQLUpdateStatement.class, SchemaConfig.class, String.class, String.class, String.class, RouteResultset.class});
        method.setAccessible(true);
        try {
            method.invoke(c.newInstance(), update, schemaConfig, tableName, "ID", "", routeResultset);
            if (throwException) {
                System.out.println("未抛异常，解析通过则不对！");
                Assert.assertTrue(false);
            } else {
                System.out.println("未抛异常，解析通过，此情况分片字段可能在update语句中但是实际不会被更新");
                Assert.assertTrue(true);
            }
        } catch (Exception e) {
            if (throwException) {
                System.out.println(e.getCause().getClass());
                Assert.assertTrue(e.getCause() instanceof SQLNonTransientException);
                System.out.println("抛异常原因为SQLNonTransientException则正确");
            } else {
                System.out.println("抛异常，需要检查");
                Assert.assertTrue(false);
            }
        }
    }

    /*
    * 添加一个static方法用于打印一个SQL的where子句，比如这样的一条SQL:
    * update mytab t set t.ptn_col = 'A', col1 = 3 where ptn_col = 'A' and (col1 = 4 or col2 > 5);
    * where子句的语法树如下
    *                  AND
    *              /        \
    *             =          OR
    *          /   \       /    \
    *     ptn_col 'A'    =       >
    *                  /  \    /   \
    *               col1  4  col2   5
    * 其输出如下，(按层输出，并且每层最后输出下一层的节点数目)
    * BooleanAnd			Num of nodes in next level: 2
    * Equality	BooleanOr			Num of nodes in next level: 4
    * ptn_col	'A'	Equality	Equality			Num of nodes in next level: 4
    * col1	4	col2	5			Num of nodes in next level: 0
    *
    * 因为大部分的update的where子句都比较简单，按层次打印应该足够清晰，未来可以完全按照逻辑打印类似上面的整棵树结构
     */
    public static void printWhereClauseAST(SQLExpr sqlExpr) {
        // where子句的AST sqlExpr可以通过 MySqlUpdateStatement.getWhere(); 获得
        if (sqlExpr == null)
            return;
        ArrayList<SQLExpr> exprNode = new ArrayList<>();
        int i = 0, curLevel = 1, nextLevel = 0;
        SQLExpr iterExpr;
        exprNode.add(sqlExpr);
        while (true) {
            iterExpr = exprNode.get(i++);
            if (iterExpr == null)
                break;

            if (iterExpr instanceof SQLBinaryOpExpr) {
                System.out.print(((SQLBinaryOpExpr) iterExpr).getOperator());
            } else {
                System.out.print(iterExpr.toString());
            }
            System.out.print("\t");
            curLevel--;

            if (iterExpr instanceof SQLBinaryOpExpr) {
                if (((SQLBinaryOpExpr) iterExpr).getLeft() != null) {
                    exprNode.add(((SQLBinaryOpExpr) iterExpr).getLeft());
                    nextLevel++;
                }
                if (((SQLBinaryOpExpr) iterExpr).getRight() != null) {
                    exprNode.add(((SQLBinaryOpExpr) iterExpr).getRight());
                    nextLevel++;
                }
            }
            if (curLevel == 0) {
                System.out.println("\t\tNum of nodes in next level: " + nextLevel);
                curLevel = nextLevel;
                nextLevel = 0;
            }
            if (exprNode.size() == i)
                break;
        }
    }
}
