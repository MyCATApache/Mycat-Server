/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.parser;

import java.sql.SQLSyntaxErrorException;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;
import org.opencloudb.mpp.ColumnRoutePair;
import org.opencloudb.mpp.JoinRel;
import org.opencloudb.mpp.SelectParseInf;
import org.opencloudb.mpp.SelectSQLAnalyser;
import org.opencloudb.mpp.ShardingParseInfo;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.QueryTreeNode;


public class TestSelectSQLAnalyser {

	@Test
	public void testSelectRoute() throws SQLSyntaxErrorException,
			StandardException {
		SelectParseInf parsInf = new SelectParseInf();
		parsInf.ctx = new ShardingParseInfo();
		Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndCondtions = null;
		Map<String, Set<ColumnRoutePair>> columnsMap = null;
		String sql = null;
		QueryTreeNode ast = null;

		sql = "select a.id,a.name,b.type from db1.a a join b on a.id=b.id where a.sharding_id=4 or a.sharding_id=5  limit 2,10";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// Assert.assertEquals(true, parsInf.isContainsSchema());
		// System.out.println("sql:" + new NodeToString().toString(ast));
		// two tables
		Assert.assertEquals(2, tablesAndCondtions.size());
		// a condtion is 2
		Assert.assertEquals(1, tablesAndCondtions.get("A").size());
		Assert.assertEquals(2,
				tablesAndCondtions.get("A").get("sharding_id".toUpperCase())
						.size());

		Assert.assertEquals(1, parsInf.ctx.joinList.size());

		parsInf.clear();
		sql = "select user";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// NO tables
		Assert.assertEquals(0, tablesAndCondtions.size());

		parsInf.ctx.tablesAndConditions.clear();
		sql = "SELECT last_name, job_id FROM demo.employees WHERE job_id = (SELECT job_id FROM employeesBack WHERE employee_id = 141) and (sharding_id='5' or sharding_id in (22,33,44))";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// 2 tables
		Assert.assertEquals(2, tablesAndCondtions.size());
		// a condtion is
		Assert.assertEquals(1, tablesAndCondtions
				.get("employees".toUpperCase()).size());
		Assert.assertEquals(4, tablesAndCondtions
				.get("employees".toUpperCase())
				.get("sharding_id".toUpperCase()).size());
		Assert.assertEquals(1,
				tablesAndCondtions.get("employeesBack".toUpperCase()).size());

		parsInf.clear();
		sql = "SELECT ID,NAME FROM Aa WHERE EXISTS (SELECT * FROM demo2.B B WHERE B.AID=3) ";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(2, tablesAndCondtions.size());
		// condtion
		Assert.assertEquals(1, tablesAndCondtions.get("B").size());

		// test table alias
		parsInf.clear();
		sql = "SELECT * FROM B baLias WHERE baLias.AID=1 ";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		// condtion
		Assert.assertEquals(1, tablesAndCondtions.get("B").size());

		// test column alias ,
		parsInf.clear();
		sql = "SELECT ID ,count(*) as count FROM B baLias WHERE baLias.AID=1 and count=5 ";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		// condtion
		Assert.assertEquals(2, tablesAndCondtions.get("B").size());
		Assert.assertEquals(1,
				tablesAndCondtions.get("B").get("count".toUpperCase()).size());
		// test select union

		sql = "select * from  offer A where a.member_id='abc' union select * from product_visit b where B.offer_id =123";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(2, tablesAndCondtions.size());
		// condtion
		Map<String, Set<ColumnRoutePair>> product_visitCond = tablesAndCondtions
				.get("product_visit".toUpperCase());
		Assert.assertEquals(1, product_visitCond.size());
		Assert.assertEquals(1, product_visitCond.get("offer_id".toUpperCase())
				.size());

		// not operater
		sql = "select * from A where not sharding_id=50";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		// condtion
		Assert.assertEquals(0, tablesAndCondtions.get("A").size());

		// in operater
		sql = "select * from A where  sharding_id  in(1,222,333)";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		columnsMap = tablesAndCondtions.get("A");
		// condtion
		Assert.assertEquals(1, columnsMap.size());
		Assert.assertEquals(3, columnsMap.get("sharding_id".toUpperCase())
				.size());

		// in operater
		sql = "select * from A where  sharding_id  in('222','333')";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		columnsMap = tablesAndCondtions.get("A");
		// condtion
		Assert.assertEquals(1, columnsMap.size());
		Assert.assertEquals(2, columnsMap.get("sharding_id".toUpperCase())
				.size());

		// not operater 2
		sql = "select * from A where  sharding_id not in(222,333)";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		// condtion
		Assert.assertEquals(0, tablesAndCondtions.get("A").size());
		//
		// not join
		sql = "select * from A where  sharding_id not in(select id from B)";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(2, tablesAndCondtions.size());
		// condtion
		Assert.assertEquals(0, tablesAndCondtions.get("A").size());
		//
		// not join
		sql = "select * from COMPANY where  (sharding_id=10000 ) or not (sharding_id=10010)";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		// condtion
		columnsMap = tablesAndCondtions.get("COMPANY");
		Assert.assertEquals(1, columnsMap.size());
		Assert.assertEquals(1, columnsMap.get("sharding_id".toUpperCase())
				.size());

		sql = "select * from COMPANY where  (sharding_id=10000 ) or  (sharding_id=10010)";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		// condtion
		Assert.assertEquals(
				2,
				tablesAndCondtions.get("COMPANY")
						.get("sharding_id".toUpperCase()).size());

		sql = "select * from offer where (offer_id, group_id ) in ((123,234),(222,444))";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());

		Map<String, Set<ColumnRoutePair>> offerCondMap = tablesAndCondtions
				.get("OFFER");
		// condtion
		Assert.assertEquals(2, offerCondMap.get("offer_id".toUpperCase())
				.size());
		Assert.assertEquals(2, offerCondMap.get("group_id".toUpperCase())
				.size());

		sql = "SELECT * FROM offer WHERE FALSE OR offer_id = 123 AND member_ID = 123 OR member_id = 123 AND member_id = 234 OR member_id = 123 AND member_id = 345 OR member_id = 123 AND member_id = 456 OR offer_id = 234 AND group_id = 123 OR offer_id = 234 AND group_id = 234 OR offer_id = 234 AND group_id = 345 OR offer_id = 234 AND group_id = 456 OR offer_id = 345 AND group_id = 123 OR offer_id = 345 AND group_id = 234 OR offer_id = 345 AND group_id = 345 OR offer_id = 345 AND group_id = 456 OR offer_id = 456 AND group_id = 123 OR offer_id = 456 AND group_id = 234 OR offer_id = 456 AND group_id = 345 OR offer_id = 456 AND group_id = 456";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		offerCondMap = tablesAndCondtions.get("OFFER");
		Assert.assertEquals(4, offerCondMap.get("member_id".toUpperCase())
				.size());

		sql = "select * from(select * from offer_detail where custmer='Mr I') offer,mydb.B where offer.id=B.id and b.columA=3333";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(2, tablesAndCondtions.size());
		offerCondMap = tablesAndCondtions.get("offer_detail".toUpperCase());
		Assert.assertEquals(1, offerCondMap.get("custmer".toUpperCase()).size());

		sql = "select count(*) from (select * from(select * from offer_detail where offer_id='123' or offer_id='234' limit 88)offer  where offer.member_id='abc' limit 60) w "
				+ " where w.member_id ='pavarotti17' limit 99";
		// sql="select * from(select * from offer_detail where offer_id='123' or offer_id='234' limit 88)offer  where offer.member_id='abc'";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		offerCondMap = tablesAndCondtions.get("offer_detail".toUpperCase());
		Assert.assertEquals(2, offerCondMap.get("offer_id".toUpperCase())
				.size());

		sql = "select * from wp_image where `seLect`='pavarotti17' ";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		// tables
		Assert.assertEquals(1, tablesAndCondtions.size());
		offerCondMap = tablesAndCondtions.get("wp_image".toUpperCase());
		Assert.assertEquals(1, offerCondMap.get("seLect".toUpperCase()).size());

		sql = "select * from customer,orders where customer.id=orders.customer_id";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;

		sql = "select o.*,d.* from (select * from ordera) o left join(select * from download) d on d.currentdate = o.currentdate";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		Assert.assertEquals(2, tablesAndCondtions.size());
		Assert.assertEquals(0, tablesAndCondtions.get("ordera".toUpperCase())
				.size());
		Assert.assertEquals(0, tablesAndCondtions.get("download".toUpperCase())
				.size());
		Assert.assertEquals(
				"download".toUpperCase() + "." + "currentdate".toUpperCase()
						+ "=" + "ordera".toUpperCase() + "."
						+ "currentdate".toUpperCase(),
				parsInf.ctx.joinList.get(0).joinSQLExp);

		sql = "select i.*,Description,DescriptionType,QuestionNumber,Name,RelationType,q.Gender,q.Birthday,q.DepartmentName,q.SimpleDescription,q.StateDescription,q.ExamedTag,q.ExamedDescription,q.DiseaseTag,q.IsSystem,q.Attachment,q.Age,q.AgeType,q.LocalID,ap.NickName AS PatientName,ad.Realname AS DoctorName from bjd_consult_inquiry i left join bjd_consult_question q on q.QuestionID=i.QuestionID left join bjd_account ap on ap.AccountID=i.PatientID left join bjd_doctor ad on ad.AccountID=i.DoctorID where q.LocalID='301002' and q.IsSystem=true and i.State in (6,5,9) order by q.IsSystem desc limit 0,5";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		Assert.assertEquals(4, tablesAndCondtions.size());
		Assert.assertEquals(3, parsInf.ctx.joinList.size());
		Assert.assertEquals(4, parsInf.ctx.tableAliasMap.size());
		Assert.assertEquals(1, parsInf.ctx.tablesAndConditions.get("BJD_CONSULT_INQUIRY").size());
		Assert.assertEquals(2, parsInf.ctx.tablesAndConditions.get("BJD_CONSULT_QUESTION").size());
		
		
		sql = "select distinct c.*,l.Name LocalName from  tablea c left join bjd_local l on c.LocalID=l.LocalID where ContentID='xxxxxb838'";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		Assert.assertEquals(2, tablesAndCondtions.size());
		Assert.assertEquals(true, tablesAndCondtions.get("TABLEA").containsKey("CONTENTID"));

		sql="SELECT  A.names FROM (SELECT *  FROM (SELECT * FROM customer   WHERE sharding_ID = '10000') B  LEFT JOIN (SELECT  NAME  NAMES FROM employee WHERE sharding_ID = '10000') C ON B.name = C.names UNION ALL SELECT *  FROM (SELECT * FROM customer  WHERE sharding_ID = '10000') B LEFT JOIN (SELECT NAME  NAMES FROM employee WHERE sharding_ID = '10000') C  ON B.name = C.names) AS A ORDER BY NAME DESC";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		Assert.assertEquals(2, tablesAndCondtions.size());
		
		
		
		sql="select * from T1 inner join T2 on T1.id = T2.id and T2.type=T1.type";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		Assert.assertEquals(1, parsInf.ctx.joinList.size());
		Assert.assertEquals(new JoinRel("T1","id","T2","id"), parsInf.ctx.joinList.get(0));
		Assert.assertEquals(2, tablesAndCondtions.size());
		
		
		
		sql="select * from T1 inner join T2 on T1.id = T2.id or T2.type=T1.type";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		Assert.assertEquals(1, parsInf.ctx.joinList.size());
		Assert.assertEquals(new JoinRel("T1","id","T2","id"), parsInf.ctx.joinList.get(0));
		Assert.assertEquals(2, tablesAndCondtions.size());
		
		
		sql="SELECT * from ismp_ocs_record_sn_ocs_in where cycle_id=null";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		SelectSQLAnalyser.analyse(parsInf, ast);
		tablesAndCondtions = parsInf.ctx.tablesAndConditions;
		Assert.assertEquals(0, parsInf.ctx.tablesAndConditions.get("ismp_ocs_record_sn_ocs_in".toUpperCase()).size());

		

	}
}