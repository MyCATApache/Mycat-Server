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

import com.foundationdb.sql.parser.ConstantNode;
import com.foundationdb.sql.parser.InsertNode;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.ResultSetNode;
import com.foundationdb.sql.parser.RowResultSetNode;
import com.foundationdb.sql.parser.RowsResultSetNode;
import com.foundationdb.sql.parser.ValueNode;

/**
 * insert sql analyser
 * 
 * @author wuzhih
 * 
 */
// INSERT [LOW_PRIORITY |DELAYED| HIGH_PRIORITY] [IGNORE]
// [INTO]tbl_name[(col_name,...)] VALUES ({expr| DEFAULT},...),(...),... [ON
// DUPLICATE KEY UPDATEcol_name=expr, ... ]

// insert into table1 select * FROM table2 WHERE id not in ( select id from
// table1)

public class InsertSQLAnalyser {

	public static InsertParseInf analyse(QueryTreeNode ast)
			throws SQLSyntaxErrorException {
		InsertNode insrtNode = (InsertNode) ast;
		String targetTable = insrtNode.getTargetTableName().getTableName()
				.toUpperCase();
		InsertParseInf parsInf = new InsertParseInf();
		// must linked hash map to keep sequnce
		Map<String, String> colMap = new LinkedHashMap<String, String>();
		parsInf.columnPairMap = colMap;
		ResultColumnList columList = insrtNode.getTargetColumnList();
		String[] columnNames = null;
		if (columList != null) {
			columnNames = columList.getColumnNames();
		}
		ResultSetNode resultSetNode = insrtNode.getResultSetNode();
		if (resultSetNode instanceof RowResultSetNode) {
			RowResultSetNode rowSetNode = (RowResultSetNode) resultSetNode;
			parseInsertParams(colMap, columnNames, rowSetNode);
		} else if (resultSetNode instanceof RowsResultSetNode) {
			throw new SQLSyntaxErrorException("insert multi rows not supported");

		} else {
			parsInf.fromQryNode = resultSetNode;
		}
		parsInf.tableName = targetTable;
		return parsInf;
	}

	private static void parseInsertParams(Map<String, String> colMap,
			String[] columnNames, RowResultSetNode rowSetNode) {
		ResultColumnList colList = rowSetNode.getResultColumns();
		int size = columnNames.length;
		for (int i = 0; i < size; i++) {
			ValueNode expNode = colList.get(i).getExpression();
			if (expNode instanceof ConstantNode) {
				Object value = ((ConstantNode) expNode).getValue();
				if (value != null) {
					String colVale = value.toString();
					colMap.put(columnNames[i].toUpperCase(), colVale);
				}

				// System.out.println(columnNames[i] + " " + colVale);
			} else {
				colMap.put(columnNames[i].toUpperCase(), "?");
//				System.out.println("todo column value class:"
//						+ expNode.getClass().getCanonicalName());
			}

		}
	}
}