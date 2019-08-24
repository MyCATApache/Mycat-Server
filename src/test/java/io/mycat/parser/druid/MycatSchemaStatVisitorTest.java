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

	String sql = "select count(*) count from (select *\r\n"
      + "          from (select b.sales_count,\r\n" + "                       b.special_type,\r\n"
      + "                       a.prod_offer_id offer_id,\r\n"
      + "                       a.alias_name as offer_name,\r\n"
      + "                       (select c.attr_value_name\r\n"
      + "                          from attr_value c\r\n"
      + "                         where c.attr_id = '994001448'\r\n"
      + "                           and c.attr_value = b.special_type) as attr_value_name,\r\n"
      + "                       a.offer_type offer_kind,\r\n"
      + "                       a.offer_comments,\r\n" + "                       a.is_ge\r\n"
      + "                  from prod_offer a, special_offer b\r\n"
      + "                 where a.prod_offer_id = b.prod_offer_id\r\n"
      + "                   and (a.offer_type = '11' or a.offer_type = '10')\r\n"
      + "                   and (b.region_id = '731' or b.region_id = '10000000')\r\n"
      + "                   and a.status_cd = '10'\r\n"
      + "                   and b.special_type = '0'\r\n" + "                union all\r\n"
      + "                select b.sales_count,\r\n" + "                       b.special_type,\r\n"
      + "                       a.prod_offer_id offer_id,\r\n"
      + "                       a.alias_name as offer_name,\r\n"
      + "                       (select c.attr_value_name\r\n"
      + "                          from attr_value c\r\n"
      + "                         where c.attr_id = '994001448'\r\n"
      + "                           and c.attr_value = b.special_type) as attr_value_name,\r\n"
      + "                       a.offer_type offer_kind,\r\n"
      + "                       a.offer_comments,\r\n" + "                       a.is_ge\r\n"
      + "                  from prod_offer a, special_offer b\r\n"
      + "                 where a.prod_offer_id = b.prod_offer_id\r\n"
      + "                   and (a.offer_type = '11' or a.offer_type = '10')\r\n"
      + "                   and (b.region_id = '731' or b.region_id = '10000000')\r\n"
      + "                   and a.status_cd = '10'\r\n"
      + "                   and b.special_type = '1'\r\n"
      + "                   and exists (select 1\r\n"
      + "                          from prod_offer_channel l\r\n"
      + "                         where a.prod_offer_id = l.prod_offer_id\r\n"
      + "                           and l.channel_id = '11')\r\n"
      + "                   and not exists\r\n" + "                 (select 1\r\n"
      + "                          from product_offer_cat ml\r\n"
      + "                         where ml.prod_offer_id = a.prod_offer_id\r\n"
      + "                           and ml.offer_cat_type = '89')\r\n"
      + "                   and (exists (select 1\r\n"
      + "                                  from sales_restrication\r\n"
      + "                                 where object_id = a.prod_offer_id\r\n"
      + "                                   and domain_id = '1965868'\r\n"
      + "                                   and restrication_flag = '0'\r\n"
      + "                                   and domain_type = '19F'\r\n"
      + "                                   and state = '00A') or exists\r\n"
      + "                        (select 1\r\n"
      + "                           from sales_restrication\r\n"
      + "                          where object_id = a.prod_offer_id\r\n"
      + "                            and domain_id = '843073100000000'\r\n"
      + "                            and restrication_flag = '0'\r\n"
      + "                            and domain_type = '19E'\r\n"
      + "                            and state = '00A') or exists\r\n"
      + "                        (select 1\r\n"
      + "                           from sales_restrication\r\n"
      + "                          where object_id = a.prod_offer_id\r\n"
      + "                            and domain_id = '1965868'\r\n"
      + "                            and restrication_flag = '0'\r\n"
      + "                            and domain_type = '19X'\r\n"
      + "                            and state = '00A'\r\n"
      + "                            and (max_value = 1 or min_value = 1)\r\n"
      + "                            and extended_field = '731') or exists\r\n"
      + "                        (select 1\r\n"
      + "                           from sales_restrication\r\n"
      + "                          where object_id = a.prod_offer_id\r\n"
      + "                            and domain_id = concat('-', '11')\r\n"
      + "                            and restrication_flag = '0'\r\n"
      + "                            and domain_type = '19F'\r\n"
      + "                            and state = '00A') or not exists\r\n"
      + "                        (select 1\r\n"
      + "                           from sales_restrication\r\n"
      + "                          where object_id = a.prod_offer_id\r\n"
      + "                            and restrication_flag = '0'\r\n"
      + "                            and (domain_type in ('19F', '19E') or\r\n"
      + "                                (domain_type = '19X' and\r\n"
      + "                                extended_field = '731' and\r\n"
      + "                                (max_value = 1 or min_value = 1)))\r\n"
      + "                            and state = '00A'))\r\n"
      + "                   and not exists (select 1\r\n"
      + "                          from sales_restrication\r\n"
      + "                         where object_id = a.prod_offer_id\r\n"
      + "                           and domain_id = '1965868'\r\n"
      + "                           and restrication_flag = '1'\r\n"
      + "                           and domain_type = '19F'\r\n"
      + "                           and state = '00A')\r\n"
      + "                   and not exists (select 1\r\n"
      + "                          from sales_restrication\r\n"
      + "                         where object_id = a.prod_offer_id\r\n"
      + "                           and domain_id = '843073100000000'\r\n"
      + "                           and restrication_flag = '1'\r\n"
      + "                           and domain_type = '19E'\r\n"
      + "                           and state = '00A')\r\n"
      + "                   and not exists\r\n" + "                 (select 1\r\n"
      + "                          from sales_restrication\r\n"
      + "                         where object_id = a.prod_offer_id\r\n"
      + "                           and domain_id = '1965868'\r\n"
      + "                           and restrication_flag = '1'\r\n"
      + "                           and domain_type = '19X'\r\n"
      + "                           and state = '00A'\r\n"
      + "                           and (min_value = 1 or max_value = 1)\r\n"
      + "                           and extended_field = '731')\r\n"
      + "                   and not exists (select 1\r\n"
      + "                          from sales_restrication\r\n"
      + "                         where object_id = a.prod_offer_id\r\n"
      + "                           and domain_id = concat('-', '11')\r\n"
      + "                           and restrication_flag = '1'\r\n"
      + "                           and domain_type = '19F'\r\n"
      + "                           and state = '00A')\r\n" + "                   and exists\r\n"
      + "                 (select 1\r\n" + "                          from prod_offer_region v1\r\n"
      + "                         where v1.prod_offer_id = a.prod_offer_id\r\n"
      + "                           and (v1.common_region_id = '731' or\r\n"
      + "                               v1.common_region_id = '10000000' or\r\n"
      + "                               v1.common_region_id = '73101'))) t\r\n"
      + "         order by t.sales_count desc)";

	/**
   * 8层以上 嵌套or语句
   */
  @Test
  public void test5() {
    List<List<Condition>> list = getConditionList(sql);

    Assert.assertTrue(list.size() < 100);
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
