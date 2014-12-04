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

import junit.framework.Assert;

import org.junit.Test;
import org.opencloudb.mpp.MergeCol;
import org.opencloudb.mpp.OrderCol;
import org.opencloudb.mpp.SelectParseInf;
import org.opencloudb.mpp.SelectSQLAnalyser;
import org.opencloudb.mpp.ShardingParseInfo;
import org.opencloudb.route.RouteResultset;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.QueryTreeNode;

public class MergeSQLParserTest {
	@Test
	public void testSQL() throws SQLSyntaxErrorException, StandardException {
		SelectParseInf parsInf = new SelectParseInf();
		parsInf.ctx = new ShardingParseInfo();
		String sql = null;
		QueryTreeNode ast = null;

		// test order by parse
		sql = "select o.* from Orders o   group by o.name order by o.id asc ,o.age desc limit 5,10";
		parsInf.clear();
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);

		SelectSQLAnalyser.analyse(parsInf, ast);
		RouteResultset rrs = new RouteResultset(sql,0);
		String sql2 = SelectSQLAnalyser.analyseMergeInf(rrs, ast, true,-1);
		Assert.assertEquals(
				"SELECT o.* FROM orders AS o GROUP BY o.name ORDER BY o.id, o.age DESC LIMIT 15 OFFSET 0",
				sql2);

		Assert.assertEquals("name", rrs.getGroupByCols()[0]);
		Assert.assertEquals(Integer.valueOf(OrderCol.COL_ORDER_TYPE_ASC), rrs.getOrderByCols().get("id"));
		Assert.assertEquals(5, rrs.getLimitStart());
		Assert.assertEquals(10, rrs.getLimitSize());

		sql = "select o.name,count(o.id) as total, max(o.mx) as maxOders,sum(MOD2(29,9)),min(o.price) from Orders o   group by name";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		rrs = new RouteResultset(sql,0);
		SelectSQLAnalyser.analyseMergeInf(rrs, ast, false,-1);
		Assert.assertEquals(true, rrs.isHasAggrColumn());
		Assert.assertEquals(2, rrs.getMergeCols().size());
		sql2 = SelectSQLAnalyser.analyseMergeInf(rrs, ast, false,-1);
		
		// aggregate column should has alias in order to used in oder by clause
		sql = "select  count(*)   from orders order by count(*) desc";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		rrs = new RouteResultset(sql,0);
		SQLSyntaxErrorException e=null;
		try
		{
		SelectSQLAnalyser.analyseMergeInf(rrs, ast, false,-1);
		}catch(SQLSyntaxErrorException e1)
		{
			e=e1;
		}
		Assert.assertNotNull(e);
		Assert.assertEquals(true, rrs.isHasAggrColumn());
		
		
		
		// aggregate column should has alias in order to used in oder by clause
				sql = "select  count(*)  as total from orders order by total desc";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		rrs = new RouteResultset(sql,0);
		SelectSQLAnalyser.analyseMergeInf(rrs, ast, false,-1);
		Assert.assertEquals(true, rrs.isHasAggrColumn());
		Assert.assertEquals(Integer.valueOf(OrderCol.COL_ORDER_TYPE_DESC), rrs.getOrderByCols().get("total"));
		
		
		// order by column has alias and should be 'modified' to alias 
		sql = "select id as myid from person order by id";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		rrs = new RouteResultset(sql,0);
		SelectSQLAnalyser.analyseMergeInf(rrs, ast, false,-1);
		Assert.assertEquals(Integer.valueOf(OrderCol.COL_ORDER_TYPE_ASC), rrs.getOrderByCols().get("myid"));
		
		
		// aggregate column has alias
		sql = "select counT(*)  as TOTaL from person order by toTal";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		rrs = new RouteResultset(sql,0);
		SelectSQLAnalyser.analyseMergeInf(rrs, ast, false,-1);
		Map<String,Integer> mergeCols=rrs.getMergeCols();
		Assert.assertEquals(1,mergeCols.size());
		Assert.assertEquals(Integer.valueOf(MergeCol.MERGE_COUNT),mergeCols.get("total"));
		Assert.assertEquals(1,rrs.getOrderByCols().size());
		Assert.assertEquals(Integer.valueOf(OrderCol.COL_ORDER_TYPE_ASC),rrs.getOrderByCols().get("total"));

	}
}