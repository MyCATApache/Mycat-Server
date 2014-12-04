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
import org.opencloudb.mpp.DeleteParsInf;
import org.opencloudb.mpp.DeleteSQLAnalyser;

import com.foundationdb.sql.parser.QueryTreeNode;

public class TestDeleteSQLAnalyser {
	@Test
	public void testSQL() throws SQLSyntaxErrorException {
		String sql = null;
		QueryTreeNode ast = null;
		DeleteParsInf parsInf = null;

		sql = "delete from A";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = DeleteSQLAnalyser.analyse(ast);
		Assert.assertEquals("A".toUpperCase(), parsInf.tableName);
		Assert.assertNull("should no where condiont", parsInf.ctx);

		sql = "delete from A where A.id=10000";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		parsInf = DeleteSQLAnalyser.analyse(ast);
		Assert.assertEquals("A".toUpperCase(), parsInf.tableName);
		Assert.assertEquals(1, parsInf.ctx.tablesAndConditions.size());

	}

}