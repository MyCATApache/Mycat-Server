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
package org.opencloudb.mpp;

import java.sql.SQLSyntaxErrorException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.DeleteNode;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.SelectNode;

/**
 * delete sql analyser
 * 
 * @author wuzhih
 * 
 */

public class DeleteSQLAnalyser {

	public static DeleteParsInf analyse(QueryTreeNode ast)
			throws SQLSyntaxErrorException {
		DeleteNode deleteNode = (DeleteNode) ast;
		String targetTable = deleteNode.getTargetTableName().getTableName()
				.toUpperCase();
		DeleteParsInf parsInf = new DeleteParsInf();
		ShardingParseInfo ctx = null;
		parsInf.tableName = targetTable;
		SelectNode selNode = (SelectNode) deleteNode.getResultSetNode();
		if (selNode.getWhereClause() != null) {
			// anlayse where condition
			if (ctx == null) {
				ctx = new ShardingParseInfo();
				parsInf.ctx = ctx;
			}
			Map<String, Set<ColumnRoutePair>> tableCondMap = new LinkedHashMap<String, Set<ColumnRoutePair>>();
			ctx.tablesAndConditions.put(targetTable, tableCondMap);
			try {
				SelectSQLAnalyser.analyseWhereCondition(parsInf, false,
						targetTable, selNode.getWhereClause());
			} catch (StandardException e) {
				throw new SQLSyntaxErrorException(e);
			}
			parsInf.ctx = ctx;
		}
		return parsInf;
	}

}