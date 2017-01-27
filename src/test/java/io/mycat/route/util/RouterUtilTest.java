package io.mycat.route.util;

import io.mycat.util.StringUtil;
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
    public void testBatchInsert()  {
        String sql = "insert into hotnews(title,name) values('test1',\"name\"),('(test)',\"(test)\"),('\\\"',\"\\'\"),(\")\",\"\\\"\\')\");";
        List<String> values = RouterUtil.handleBatchInsert(sql, sql.toUpperCase().indexOf("VALUES"));
        Assert.assertTrue(values.get(0).equals("insert into hotnews(title,name) values('test1',\"name\")"));
        Assert.assertTrue(values.get(1).equals("insert into hotnews(title,name) values('(test)',\"(test)\")"));
        Assert.assertTrue(values.get(2).equals("insert into hotnews(title,name) values('\\\"',\"\\'\")"));
        Assert.assertTrue(values.get(3).equals("insert into hotnews(title,name) values(\")\",\"\\\"\\')\")"));
    }


    @Test
    public void testRemoveSchema()  {
        String sql = "update test set name='abcdtestx.aa'   where id=1 and testx=123";

      String afterAql=  RouterUtil.removeSchema(sql,"testx");
        Assert.assertEquals(sql,afterAql);
        System.out.println(afterAql);

    }
    @Test
    public void testRemoveSchemaSelect()  {
        String sql = "select id as 'aa' from  test where name='abcdtestx.aa'   and id=1 and testx=123";

        String afterAql=  RouterUtil.removeSchema(sql,"testx");
        Assert.assertEquals(sql,afterAql);

    }

    @Test
    public void testRemoveSchemaSelect2()  {
        String sql = "select id as 'aa' from  testx.test where name='abcd  testx.aa'   and id=1 and testx=123";

        String afterAql=  RouterUtil.removeSchema(sql,"testx");
        Assert.assertNotSame(sql.indexOf("testx."),afterAql.indexOf("testx."));

    }

    @Test
    public void testRemoveSchema2(){
        String sql = "update testx.test set name='abcd \\' testx.aa' where id=1";
        String sqltrue = "update test set name='abcd \\' testx.aa' where id=1";
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
    public void testRemoveSchema4(){
        String sql = "update testx.test set testx.name='abcd testx.aa' and testx.name2='abcd testx.aa' where testx.id=1";
        String sqltrue = "update test set name='abcd testx.aa' and name2='abcd testx.aa' where id=1";
        String sqlnew = RouterUtil.removeSchema(sql, "testx");
        Assert.assertEquals("处理错误：", sqltrue, sqlnew);
    }
    /**
     * @modification 修改支持createTable语句中包含“IF NOT EXISTS”的情况,这里测试下
     * @date 2016/12/8
     * @modifiedBy Hash Zhang
     */
    @Test
    public void testGetCreateTableStmtTableName(){
        String sql1 = StringUtil.makeString("create table if not exists producer(\n",
                "\tid int(11) primary key,\n",
                "\tname varchar(32)\n",
                ");").toUpperCase();
        String sql2 = StringUtil.makeString("create table good(\n",
                "\tid int(11) primary key,\n",
                "\tcontent varchar(32),\n",
                "\tproducer_id int(11) key\n",
                ");").toUpperCase();
        Assert.assertTrue("producer".equalsIgnoreCase(RouterUtil.getTableName(sql1, RouterUtil.getCreateTablePos(sql1, 0))));
        Assert.assertTrue("good".equalsIgnoreCase(RouterUtil.getTableName(sql2, RouterUtil.getCreateTablePos(sql2, 0))));
    }

    /**
     * @modification 针对修改RouterUtil的去除schema的方法支持` 进行测试
     * @date 2016/12/29
     * @modifiedBy Hash Zhang
     */
    @Test
    public void testRemoveSchemaWithHypha(){
        String sql1 = StringUtil.makeString("select `testdb`.`orders`.`id`, `testdb`.`orders`.`customer_id`, `testdb`.`orders`.`goods_id` from `testdb`.`orders` where testdb.`orders`.`id` = 1;").toUpperCase();
        String sql2 = StringUtil.makeString("select `testdb`.`orders`.`id`, testdb.`orders`.`customer_id`, `testdb`.`orders`.`goods_id` from testdb.`orders` where `testdb`.`orders`.`id` = 1;").toUpperCase();
        String sql3 = StringUtil.makeString("select testdb.`orders`.`id`, `testdb`.`orders`.`customer_id`, testdb.`orders`.`goods_id` from `testdb`.`orders` where testdb.`orders`.`id` = 1;").toUpperCase();
        String sql4 = StringUtil.makeString("select testdb.`orders`.`id`, testdb.`orders`.`customer_id`, testdb.`orders`.`goods_id` from testdb.`orders` where testdb.`orders`.`id` = 1;").toUpperCase();
        String result = "SELECT `ORDERS`.`ID`, `ORDERS`.`CUSTOMER_ID`, `ORDERS`.`GOODS_ID` FROM `ORDERS` WHERE `ORDERS`.`ID` = 1;";
        Assert.assertTrue(result.equals(RouterUtil.removeSchema(sql1,"testdb")));
        Assert.assertTrue(result.equals(RouterUtil.removeSchema(sql2,"testdb")));
        Assert.assertTrue(result.equals(RouterUtil.removeSchema(sql3,"testdb")));
        Assert.assertTrue(result.equals(RouterUtil.removeSchema(sql4,"testdb")));
    }
}
