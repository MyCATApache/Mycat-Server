package io.mycat.parser.druid;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.stat.TableStat.Condition;

import io.mycat.route.parser.druid.MycatSchemaStatVisitor;

/**
 * TODO: 增加描述
 * 
 * @author user
 * @date 2015-6-2 下午5:50:25
 * @version 0.1.0 
 * @copyright wonhigh.cn
 */
public class MycatSchemaStatVisitorTest {
	
	
	/**
	 * 从注解中解析 mycat schema
	 */
	@Test
	public void test4() {
		String sql = "/*!mycat:schema = helper1 */update adm_task a set a.remark = 'test' where id=1";
		Assert.assertEquals(getSchema(sql), "helper1.");
		sql = "/*!mycat:schema=helper1*/update adm_task a set a.remark = 'test' where id=1";
		Assert.assertEquals(getSchema(sql), "helper1.");
		sql = "/*!mycat:schema=  helper1*/update adm_task a set a.remark = 'test' where id=1";
		Assert.assertEquals(getSchema(sql), "helper1.");
		System.out.println(getSchema(sql));
	}
	
	
	/**
	 * 3层嵌套or语句
	 */
	@Test
	public void test1() {
		String sql = "select id from travelrecord "
    			+ " where id = 1 and ( fee=3 or days=5 or (traveldate = '2015-05-04 00:00:07.375' "
    			+ " and (user_id=2 or fee=days or fee = 0))) and id=2" ;
		List<List<Condition>> list = getConditionList(sql);
		Assert.assertEquals(list.size(), 5);
		Assert.assertEquals(list.get(0).size(), 2);
		Assert.assertEquals(list.get(1).size(), 2);
		Assert.assertEquals(list.get(2).size(), 3);
		Assert.assertEquals(list.get(3).size(), 4);
		Assert.assertEquals(list.get(4).size(), 3);
		
		Assert.assertEquals(list.get(0).get(0).toString(), "travelrecord.days = 5");
		Assert.assertEquals(list.get(0).get(1).toString(), "travelrecord.id = (1, 2)");
		
		Assert.assertEquals(list.get(1).get(0).toString(), "travelrecord.fee = 3");
		Assert.assertEquals(list.get(1).get(1).toString(), "travelrecord.id = (1, 2)");
		
		Assert.assertEquals(list.get(2).get(0).toString(), "travelrecord.fee = 0");
		Assert.assertEquals(list.get(2).get(1).toString(), "travelrecord.traveldate = 2015-05-04 00:00:07.375");
		Assert.assertEquals(list.get(2).get(2).toString(), "travelrecord.id = (1, 2)");
		
		Assert.assertEquals(list.get(3).get(0).toString(), "travelrecord.fee = null");
		Assert.assertEquals(list.get(3).get(1).toString(), "travelrecord.days = null");
		Assert.assertEquals(list.get(3).get(2).toString(), "travelrecord.traveldate = 2015-05-04 00:00:07.375");
		Assert.assertEquals(list.get(3).get(3).toString(), "travelrecord.id = (1, 2)");
		
		Assert.assertEquals(list.get(4).get(0).toString(), "travelrecord.user_id = 2");
		Assert.assertEquals(list.get(4).get(1).toString(), "travelrecord.traveldate = 2015-05-04 00:00:07.375");
		Assert.assertEquals(list.get(4).get(2).toString(), "travelrecord.id = (1, 2)");

		System.out.println(list.size());
	}
	
	/**
	 * 1层嵌套or语句
	 */
	@Test
	public void test2() {
		String sql = "select id from travelrecord "
    			+ " where id = 1 and ( fee=3 or days=5 or name = 'zhangsan')" ;
		List<List<Condition>> list = getConditionList(sql);
		
		Assert.assertEquals(list.size(), 3);
		Assert.assertEquals(list.get(0).size(), 2);
		Assert.assertEquals(list.get(1).size(), 2);
		Assert.assertEquals(list.get(2).size(), 2);

		
		Assert.assertEquals(list.get(0).get(0).toString(), "travelrecord.name = zhangsan");
		Assert.assertEquals(list.get(0).get(1).toString(), "travelrecord.id = 1");
		
		Assert.assertEquals(list.get(1).get(0).toString(), "travelrecord.days = 5");
		Assert.assertEquals(list.get(1).get(1).toString(), "travelrecord.id = 1");
		
		Assert.assertEquals(list.get(2).get(0).toString(), "travelrecord.fee = 3");
		Assert.assertEquals(list.get(2).get(1).toString(), "travelrecord.id = 1");
	}
	
	/**
	 * 1层嵌套or语句
	 */
	@Test
	public void test3() {
		String sql = "select id from travelrecord "
    			+ " where id = 1 and fee=3 or days=5 or name = 'zhangsan'" ;
		List<List<Condition>> list = getConditionList(sql);
		
		Assert.assertEquals(list.size(), 3);
		
		Assert.assertEquals(list.get(0).size(), 1);
		Assert.assertEquals(list.get(1).size(), 1);
		Assert.assertEquals(list.get(2).size(), 2);

		Assert.assertEquals(list.get(0).get(0).toString(), "travelrecord.name = zhangsan");
		
		Assert.assertEquals(list.get(1).get(0).toString(), "travelrecord.days = 5");
		
		Assert.assertEquals(list.get(2).get(0).toString(), "travelrecord.id = 1");
		Assert.assertEquals(list.get(2).get(1).toString(), "travelrecord.fee = 3");
	}
	
	private String getSchema(String sql) {
		SQLStatementParser parser =null;
		parser = new MySqlStatementParser(sql);

		MycatSchemaStatVisitor visitor = null;
		SQLStatement statement = null;
		//解析出现问题统一抛SQL语法错误
		try {
			statement = parser.parseStatement();
            visitor = new MycatSchemaStatVisitor();
		} catch (Exception e) {
			e.printStackTrace();
		}
		statement.accept(visitor);
		
		
		return visitor.getCurrentTable();
	}

	private List<List<Condition>> getConditionList(String sql) {
		SQLStatementParser parser =null;
		parser = new MySqlStatementParser(sql);

		MycatSchemaStatVisitor visitor = null;
		SQLStatement statement = null;
		//解析出现问题统一抛SQL语法错误
		try {
			statement = parser.parseStatement();
            visitor = new MycatSchemaStatVisitor();
		} catch (Exception e) {
			e.printStackTrace();
		}
		statement.accept(visitor);
		
		List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
		if(visitor.hasOrCondition()) {//包含or语句
			//TODO
			//根据or拆分
			mergedConditionList = visitor.splitConditions();
		} else {//不包含OR语句
			mergedConditionList.add(visitor.getConditions());
		}
		
		return mergedConditionList;
	}
}
