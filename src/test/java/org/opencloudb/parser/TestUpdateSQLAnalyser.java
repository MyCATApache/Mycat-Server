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
import org.opencloudb.mpp.JoinRel;
import org.opencloudb.mpp.UpdateParsInf;
import org.opencloudb.mpp.UpdateSQLAnalyser;

import com.foundationdb.sql.parser.QueryTreeNode;

public class TestUpdateSQLAnalyser {
	@Test
	public void testUpdateSQL() throws SQLSyntaxErrorException {
		String sql = null;
		QueryTreeNode ast = null;
		UpdateParsInf parsInf = null;
		
		sql = "update A set A.qcye='aaaaa',A.colm2='ddd'";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = UpdateSQLAnalyser.analyse(ast);
		Assert.assertEquals("A".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(2, parsInf.columnPairMap.size());
		Assert.assertNull("should no where condiont", parsInf.ctx);
		
		sql = "update A set A.qcye=B.qcye,A.colm2='ddd', colm3=5555 where A.kmdm=B.kmdm and   A.fmonth=B.fmonth and   A.fmonth=0";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = UpdateSQLAnalyser.analyse(ast);
		Assert.assertEquals("A".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(3, parsInf.columnPairMap.size());
		Assert.assertEquals(2, parsInf.ctx.tablesAndConditions.size());
		Assert.assertEquals(1, parsInf.ctx.tablesAndConditions.get("A").size());
		Assert.assertEquals(0, parsInf.ctx.tablesAndConditions.get("B").size());
		Assert.assertEquals(2, parsInf.ctx.joinList.size());
		Assert.assertEquals(new JoinRel("A", "kmdm", "B", "kmdm"),
				parsInf.ctx.joinList.get(0));

		// Assert.assertNotNull(parsInf.fromQryNode);

		sql = "UPDATE db1.A SET HIGH=B.NEW where  A.HIGH=B.OLD";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = UpdateSQLAnalyser.analyse(ast);
		Assert.assertEquals("A", parsInf.tableName);
		Assert.assertEquals(1, parsInf.columnPairMap.size());
		Assert.assertEquals("?", parsInf.columnPairMap.get("HIGH"));
		Assert.assertEquals(2, parsInf.ctx.tablesAndConditions.size());
		Assert.assertEquals(1, parsInf.ctx.joinList.size());
		Assert.assertEquals(new JoinRel("A", "HIGH", "B", "OLD"),
				parsInf.ctx.joinList.get(0));

		sql = "Update HouseInfo Set UpdateTime = 'now',I_Valid='\"&I_Valid&\"' Where I_ID In (1,2,3)";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = UpdateSQLAnalyser.analyse(ast);
		Assert.assertEquals("HouseInfo".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(2, parsInf.columnPairMap.size());
		Assert.assertEquals("?",
				parsInf.columnPairMap.get("I_Valid".toUpperCase()));
		Assert.assertEquals(3,
				parsInf.ctx.tablesAndConditions.get("HouseInfo".toUpperCase())
						.get("I_ID").size());
		
		// test update table with alias name
		sql = "update employee a set a.name = 'system233' where a.id=3";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = UpdateSQLAnalyser.analyse(ast);
		Assert.assertEquals("employee".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(1, parsInf.columnPairMap.size());
		Assert.assertEquals("A", parsInf.tableNameAlias);
		
		sql = "update employee a set a.name = 'sys' where a.id in " +
				"(select id from (select b.id from employee b where b.id=3) as tmp)";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = UpdateSQLAnalyser.analyse(ast);
		Assert.assertEquals("employee".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(1, parsInf.columnPairMap.size());
		Assert.assertEquals("A", parsInf.tableNameAlias);
	}
}