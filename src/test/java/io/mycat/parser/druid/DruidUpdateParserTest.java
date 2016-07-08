package io.mycat.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
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
    @Test
    public void testAliasUpdate() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        MySqlStatementParser parser = new MySqlStatementParser("update hotnews h set h.id = 1 where h.name = 234;");
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
        RouteResultset routeResultset = new RouteResultset("update hotnews h set h.id = 1 where h.name = 234;", 11);
        Class c = DruidUpdateParser.class;
        Method method = c.getDeclaredMethod("confirmShardColumnNotUpdated", new Class[]{List.class, SchemaConfig.class, String.class, String.class, String.class, RouteResultset.class});
        method.setAccessible(true);
        try {
            method.invoke(c.newInstance(), update.getItems(), schemaConfig, tableName, "ID", "", routeResultset);
            System.out.println("未抛异常，解析通过则不对！");
            Assert.assertTrue(false);
        }catch (Exception e){
            System.out.println("抛异常原因为SQLNonTransientException则正确");
            Assert.assertTrue(e.getCause() instanceof SQLNonTransientException);
        }
    }
}
