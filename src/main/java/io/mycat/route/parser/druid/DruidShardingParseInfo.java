package io.mycat.route.parser.druid;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Name;

import io.mycat.route.util.RouterUtil;
import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.sqlengine.mpp.RangeValue;

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

	private SchemaStatVisitor visitor;

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
	
	public void setVisitor(SchemaStatVisitor visitor) {
		
		this.visitor = visitor;
	}
	
	public SchemaStatVisitor getVisitor(){
		
		return this.visitor;
	}

	public void addTables(Map<Name, TableStat> map) {
		
		int dotIndex;
		for(Name _name : map.keySet()){
			
			String _tableName = _name.getName().toString().toUpperCase();
			//系统表直接跳过，路由到默认datanode
			if(RouterUtil.isSystemSchema(_tableName)){
				continue;
			}
			if((dotIndex = _tableName.indexOf('.')) != -1){
				_tableName = _tableName.substring(dotIndex + 1);
			}
			addTable(_tableName);
		}
	}

}
