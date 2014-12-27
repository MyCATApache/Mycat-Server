package org.opencloudb.parser.druid;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opencloudb.mpp.ColumnRoutePair;
import org.opencloudb.mpp.RangeValue;

/**
 * druid parser result
 * @author wang.dw
 *
 */
public class DruidShardingParseInfo {
	private Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = new LinkedHashMap<String, Map<String, Set<ColumnRoutePair>>>();
	private Set<String> shardingKeySet = new HashSet<String>();
	private String sql = "";
	private List<String> tables = new ArrayList<String>();

	/**
	 * key table alias, value talbe realname;
	 */
	private Map<String, String> tableAliasMap = new LinkedHashMap<String, String>();

	public void addShardingExpr(String tableName, String columnName,
			Object value) {
		Map<String, Set<ColumnRoutePair>> tableColumnsMap = tablesAndConditions.get(tableName);
		
		if (value == null) {
			// where a=null
			return;
		}
		
		if (tableColumnsMap == null) {
			tableColumnsMap = new LinkedHashMap<String, Set<ColumnRoutePair>>();
			tablesAndConditions.put(tableName, tableColumnsMap);
		}
		
		String uperColName = columnName.toUpperCase();
		Set<ColumnRoutePair> columValues = tableColumnsMap.get(uperColName);

		if (columValues == null) {
			columValues = new LinkedHashSet<ColumnRoutePair>();
			tablesAndConditions.get(tableName).put(uperColName, columValues);
		}

		if (value instanceof Object[]) {
			for (Object item : (Object[]) value) {
				if(item == null) {
					continue;
				}
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
	}

	public Map<String, Map<String, Set<ColumnRoutePair>>> getTablesAndConditions() {
		return tablesAndConditions;
	}

	public void setTablesAndConditions(
			Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions) {
		this.tablesAndConditions = tablesAndConditions;
	}

	public Map<String, String> getTableAliasMap() {
		return tableAliasMap;
	}

	public void setTableAliasMap(Map<String, String> tableAliasMap) {
		this.tableAliasMap = tableAliasMap;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public List<String> getTables() {
		return tables;
	}

	public void addTable(String tableName) {
		this.tables.add(tableName);
	}

}
