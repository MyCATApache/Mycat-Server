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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShardingParseInfo {

	public void addShardingExpr(String tableName, String columnName,
			Object value) {
		Map<String, Set<ColumnRoutePair>> tableColumnsMap = tablesAndConditions
				.get(tableName);
		if (tableColumnsMap == null) {
			// System.out
			// .println("not found table name ,may be child select result "
			// + tableName);
			return;
		}
		if (value == null) {
			// where a=null
			return;
		}
		String uperColName = columnName.toUpperCase();
		Set<ColumnRoutePair> columValues = tableColumnsMap.get(uperColName);

		if (columValues == null) {
			columValues = new LinkedHashSet<ColumnRoutePair>();
			tablesAndConditions.get(tableName).put(uperColName, columValues);
		}

		if (value instanceof Object[]) {
			for (Object item : (Object[]) value) {
				columValues.add(new ColumnRoutePair(item.toString()));
			}
		} else if (value instanceof RangeValue) {
			columValues.add(new ColumnRoutePair((RangeValue) value));
		} else {
			columValues.add(new ColumnRoutePair(value.toString()));
		}
	}

	/**
	 * if find table name ,return table name ,else retur null
	 * 
	 * @param theName
	 * @return
	 */
	public String getTableName(String theName) {
		String upperName = theName.toUpperCase();
		if (tablesAndConditions.containsKey(upperName)) {
			return upperName;
		} else {
			upperName = tableAliasMap.get(theName);
			return (upperName == null) ? null : upperName;
		}
	}

	public void clear() {
		shardingKeySet.clear();
		tablesAndConditions.clear();
		tableAliasMap.clear();
		joinList.clear();
	}

	public Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<String, Map<String, Set<ColumnRoutePair>>>();
	public Set<String> shardingKeySet = new HashSet<String>();
	public List<JoinRel> joinList = new ArrayList<JoinRel>(1);

	/**
	 * key table alias, value talbe realname;
	 */
	public Map<String, String> tableAliasMap = new LinkedHashMap<String, String>();

	public void addJoin(JoinRel joinRel) {
		joinList.add(joinRel);

	}
}
