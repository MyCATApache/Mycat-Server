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

import junit.framework.Assert;

import org.junit.Test;
import org.opencloudb.mpp.InsertParseInf;
import org.opencloudb.mpp.InsertSQLAnalyser;

import com.foundationdb.sql.parser.QueryTreeNode;

public class TestInsertSQLAnalyser {

	@Test
	public void testInsertSQL() throws SQLSyntaxErrorException {
		String sql = null;
		QueryTreeNode ast = null;
		InsertParseInf parsInf = null;
		sql = "insert into table1  select * FROM table2 WHERE id not in ( select id from  table1) ";
		//sql = "insert into table1  select * FROM table2 WHERE id not in ( select id aT\'b  table1) ";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = InsertSQLAnalyser.analyse(ast);
		Assert.assertEquals("table1".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(0, parsInf.columnPairMap.size());
		Assert.assertNotNull(parsInf.fromQryNode);

		sql = "insert into table1(column1,column2,column3,colum4,column5,column6,column7)values('aaa',5,'1999-2-2',true,\"test\",111,55.66) ";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = InsertSQLAnalyser.analyse(ast);
		Assert.assertEquals("table1".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(7, parsInf.columnPairMap.size());
		Assert.assertNull(parsInf.fromQryNode);

		sql = "inSErt into offer_detail (`offer_id`, gmt) values (123,now())";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = InsertSQLAnalyser.analyse(ast);
		Assert.assertEquals("offer_detail".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(2, parsInf.columnPairMap.size());
		Assert.assertNull(parsInf.fromQryNode);

		sql = "insert into offer_detail (offer_id, gmt) values (0, now()), (1, now()), (2, now())";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		try {
			parsInf = InsertSQLAnalyser.analyse(ast);
		} catch (Exception e) {
			Assert.assertEquals("insert multi rows not supported",
					e.getMessage());
		}

		sql = "insert  into t_uud_user_account(USER_ID,USER_NAME,PASSWORD,CREATE_TIME,STATUS,NICK_NAME,USER_ICON_URL,USER_ICON_URL2,USER_ICON_URL3,ACCOUNT_TYPE) "
				+ "values (2488899998,'u163149830250134','af8f9dffa5d420fbc249141645b962ee','2013-12-01 00:00:00',0,NULL,NULL,NULL,NULL,1)";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = InsertSQLAnalyser.analyse(ast);
		Assert.assertEquals("t_uud_user_account".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(6, parsInf.columnPairMap.size());
		Assert.assertNull(parsInf.fromQryNode);

		// sql =
		// "delete from offer.*,wp_image.* using offer a,wp_image b where a.member_id=b.member_id and a.member_id='abc' ";

	}
}