package io.mycat.route.util;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/19
 */
public class RouterUtilTest {

    @Test
    public void testRemoveSchema1(){
        String sql = "update test set name='abcd testx.aa' where id=1";

        String sqlnew = RouterUtil.removeSchema(sql, "testx");
        Assert.assertEquals("处理错误：", sql, sqlnew);
    }

    @Test
    public void testRemoveSchema2(){
        String sql = "update testx.test set name='abcd testx.aa' where id=1";
        String sqltrue = "update test set name='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx");
        Assert.assertEquals("处理错误：", sqltrue, sqlnew);
    }

    @Test
    public void testRemoveSchema3(){
        String sql = "update testx.test set testx.name='abcd testx.aa' where testx.id=1";
        String sqltrue = "update test set name='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx");
        Assert.assertEquals("处理错误：", sqltrue, sqlnew);
    }

    @Test
    public void testBatchInsert()  {
        String sql = "insert into hotnews(title,name) values('test1',\"name\"),('(test)',\"(test)\"),('\\\"',\"\\'\"),(\")\",\"\\\"\\')\");";
        List<String> values = RouterUtil.handleBatchInsert(sql, sql.toUpperCase().indexOf("VALUES"));
        Assert.assertTrue(values.get(0).equals("insert into hotnews(title,name) values('test1',\"name\")"));
        Assert.assertTrue(values.get(1).equals("insert into hotnews(title,name) values('(test)',\"(test)\")"));
        Assert.assertTrue(values.get(2).equals("insert into hotnews(title,name) values('\\\"',\"\\'\")"));
        Assert.assertTrue(values.get(3).equals("insert into hotnews(title,name) values(\")\",\"\\\"\\')\")"));
    }

}
