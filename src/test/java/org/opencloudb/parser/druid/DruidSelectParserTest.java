package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import org.junit.Assert;
import org.junit.Test;
import org.opencloudb.parser.druid.impl.DruidSelectParser;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Hash Zhang on 2016/4/29.
 */
public class DruidSelectParserTest {
    DruidSelectParser druidSelectParser = new DruidSelectParser();

    /**
     * 此方法检测DruidSelectParser的buildGroupByCols方法是否修改了函数列
     * 因为select的函数列并不做alias处理，
     * 所以在groupby也对函数列不做修改
     *
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     */
    @Test
    public void testGroupByWithAlias() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String functionColumn = "DATE_FORMAT(h.times,'%b %d %Y %h:%i %p')";
        Map<String, String> aliaColumns = new TreeMap<>();
        SQLIdentifierExpr sqlExpr = mock(SQLIdentifierExpr.class);
        SQLIdentifierExpr expr = mock(SQLIdentifierExpr.class);
        List<SQLExpr> groupByItems = new ArrayList<>();
        groupByItems.add(sqlExpr);
        when((sqlExpr).getName()).thenReturn(functionColumn);
        Class c = DruidSelectParser.class;
        Method method = c.getDeclaredMethod("buildGroupByCols", new Class[]{List.class, Map.class});
        method.setAccessible(true);
        Object result = method.invoke(druidSelectParser, groupByItems, aliaColumns);
        Assert.assertEquals(functionColumn, ((String[]) result)[0]);
    }
}
