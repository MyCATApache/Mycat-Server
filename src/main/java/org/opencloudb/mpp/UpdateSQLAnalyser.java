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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.ColumnReference;
import com.foundationdb.sql.parser.FromList;
import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.ResultColumn;
import com.foundationdb.sql.parser.ResultColumnList;
import com.foundationdb.sql.parser.SelectNode;
import com.foundationdb.sql.parser.UpdateNode;
import com.foundationdb.sql.parser.ValueNode;

/**
 * update sql analyser
 * 
 * @author wuzhih
 * 
 */
// update A set A.qcye=B.qcye from B where A.kmdm=B.kmdm and A.fmonth=B.fmonth
// and A.fmonth=0
// UPDATE A SET HIGH=B.NEW FROM A LEFT JOIN B ON (A.HIGH=B.OLD)
// update a set HIGH=b.NEW from @SPEC1 a join @tmpDOT b on a.HIGH=b.OLD
// Update HouseInfo Set UpdateTime = '"&Now()&"',I_Valid='"&I_Valid&"' Where
// I_ID In ("&I_ID&")"
// update a set HIGH=b.NEW from SPEC1 a,tmpDOT b where a.high=b.old
// update a set high = (select new from tmpdot where old=a.high ) from spec1 a

public class UpdateSQLAnalyser {

	public static UpdateParsInf analyse(QueryTreeNode ast)
			throws SQLSyntaxErrorException {
		UpdateNode updateNode = (UpdateNode) ast;
		
		/* update table alias */
		Map<String, String> updatedTableAlias = new HashMap<String, String>();
		FromList fromList = ((SelectNode) updateNode.getResultSetNode()).getFromList();
		for (FromTable t : fromList)
		{
			String tableOrigName = t.getOrigTableName().getTableName();
			String tableAlias = t.getCorrelationName();
			if ( null==tableAlias || ""==tableAlias )
				tableAlias = tableOrigName;
				
			updatedTableAlias.put(t.getOrigTableName().getTableName().toUpperCase(), tableAlias.toUpperCase());
		}
		String targetTable = updateNode.getTargetTableName().getTableName()
				.toUpperCase();
		UpdateParsInf parsInf = new UpdateParsInf();
		ShardingParseInfo ctx = null;
		parsInf.tableName = targetTable;
		parsInf.tableNameAlias = updatedTableAlias.isEmpty() ? null : updatedTableAlias.get(parsInf.tableName);
		SelectNode selNode = (SelectNode) updateNode.getResultSetNode();
		ResultColumnList updateColumsLst = selNode.getResultColumns();
		int updateColumnSize = updateColumsLst.size();
		Map<String, String> colMap = new HashMap<String, String>(
				updateColumnSize);
		for (int i = 0; i < updateColumnSize; i++) {
			ResultColumn column = updateColumsLst.get(i);
			ColumnReference colRef = column.getReference();
			String colTableName = colRef.getTableName();
			if (colTableName != null
					&& !colTableName.toUpperCase().equals(parsInf.tableNameAlias) ) {
				throw new SQLSyntaxErrorException(
						"update multi table not supported");
			}

			ValueNode valNode = column.getExpression();
			if (valNode instanceof ColumnReference) {
				String bColTableName = ((ColumnReference) valNode)
						.getTableName();
				if (bColTableName != null
						&& !bColTableName.equalsIgnoreCase(colTableName)) {
					// A.col=B.col
					if (ctx == null) {
						ctx = new ShardingParseInfo();
						parsInf.ctx = ctx;
					}
					// and B table info
					Map<String, Set<ColumnRoutePair>> tableCondMap = new LinkedHashMap<String, Set<ColumnRoutePair>>();
					ctx.tablesAndConditions.put(bColTableName.toUpperCase(),
							tableCondMap);
				}
			}
			String columName = colRef.getColumnName().toUpperCase();
			colMap.put(columName, "?");
		}
		parsInf.columnPairMap = colMap;

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