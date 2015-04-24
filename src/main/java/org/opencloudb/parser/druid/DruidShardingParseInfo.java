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
	/**
	 * 一个sql中可能有多个WhereUnit（如子查询中的where可能导致多个）
	 */
	private List<WhereUnit> whereUnits = new ArrayList<WhereUnit>();
	
	private List<RouteCalculateUnit> routeCalculateUnits = new ArrayList<RouteCalculateUnit>();
	
	/**
	 * （共享属性）
	 */
	private String sql = "";
	
	//tables为路由计算共享属性，多组RouteCalculateUnit使用同样的tables
	private List<String> tables = new ArrayList<String>();
	
//	private RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit(this); 

	/**
	 * key table alias, value talbe realname;
	 */
	private Map<String, String> tableAliasMap = new LinkedHashMap<String, String>();

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

	public RouteCalculateUnit getRouteCalculateUnit() {
		return routeCalculateUnits.get(0);
	}
	
	public List<RouteCalculateUnit> getRouteCalculateUnits() {
		return routeCalculateUnits;
	}
	
	public void setRouteCalculateUnits(List<RouteCalculateUnit> routeCalculateUnits) {
		this.routeCalculateUnits = routeCalculateUnits;
	}
	
	public void addRouteCalculateUnit(RouteCalculateUnit routeCalculateUnit) {
		this.routeCalculateUnits.add(routeCalculateUnit);
	}
	

	public void clear() {
		for(RouteCalculateUnit unit : routeCalculateUnits ) {
			unit.clear();
		}
	}

}
