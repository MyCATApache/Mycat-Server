package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.sqlengine.mpp.RangeValue;
import io.mycat.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;

/**
 * 默认 Druid 解析
 * 对SQLStatement解析
 * 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够了，
 * 有些只能通过statement解析才能得到所有信息，有些需要通过两种方式解析才能得到完整信息
 *
 * @author wang.dw
 *
 */
public class DefaultDruidParser implements DruidParser {
	protected static final Logger LOGGER = LoggerFactory.getLogger(DefaultDruidParser.class);
	/**
	 * 解析得到的结果
	 */
	protected DruidShardingParseInfo ctx;
	
//	private Map<String,String> tableAliasMap = new HashMap<String,String>();

	private List<Condition> conditions = new ArrayList<Condition>();
	
//	public Map<String, String> getTableAliasMap() {
//		return tableAliasMap;
//	}

	public List<Condition> getConditions() {
		return conditions;
	}

	/**
	 * 解析
	 * 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @param originSql
	 * @param cachePool
	 * @param schemaStatVisitor
	 * @throws SQLNonTransientException
	 */
	@Override
	public void parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt, String originSql, LayerCachePool cachePool, MycatSchemaStatVisitor schemaStatVisitor) throws SQLNonTransientException {
		ctx = new DruidShardingParseInfo();
		//设置为原始sql，如果有需要改写sql的，可以通过修改SQLStatement中的属性，然后调用SQLStatement.toString()得到改写的sql
		ctx.setSql(originSql);
		//通过visitor解析
		visitorParse(rrs,stmt,schemaStatVisitor);

		//通过Statement解析
		statementParse(schema, rrs, stmt);

		//检测ctx中的表是否在配置的逻辑库中存在，不存在的删除
		if(ctx.getTables()!=null && ctx.getTables().size()>0){
			List<String> tables = new ArrayList<String>();
			for(String table : ctx.getTables()){
				if(schema.getTables().get(table.toUpperCase()) != null){
					tables.add(table.toUpperCase());
				}
			}
			ctx.getTables().clear();
			if(tables.size()>0){
				for(String table : tables){
					ctx.addTable(table);
				}
			}
		}
	}
	
	/**
	 * 是否终止解析,子类可覆盖此方法控制解析进程.
	 * 存在子查询的情况下,如果子查询需要先执行获取返回结果后,进一步改写sql后,再执行 在这种情况下,不再需要statement 和changeSql 解析。增加此模板方法
	 * @param schemaStatVisitor
	 * @return
	 */
	public boolean afterVisitorParser(RouteResultset rrs, SQLStatement stmt,MycatSchemaStatVisitor schemaStatVisitor){
		return false;
	}
	
	/**
	 * statement方式解析
	 * 子类可覆盖（如果visitorParse解析得不到表名、字段等信息的，就通过覆盖该方法来解析）
	 * 子类覆盖该方法一般是将SQLStatement转型后再解析（如转型为MySqlInsertStatement）
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @throws SQLNonTransientException
	 */
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		
	}
	
	/**
	 * 改写sql：加limit，加group by、加order by如有些没有加limit的可以通过该方法增加
	 * @param schema
	 * @param rrs
	 * @param stmt
	 * @param cachePool
	 * @throws SQLNonTransientException
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs,
			SQLStatement stmt,LayerCachePool cachePool) throws SQLNonTransientException {
		
	}

	/**
	 * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementPparse去解析）
	 * 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
	 *
	 * @param rrs
	 * @param stmt
	 * @param visitor
	 * @throws SQLNonTransientException
	 */
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt, MycatSchemaStatVisitor visitor) throws SQLNonTransientException{
		// 使用visitor来访问AST
		stmt.accept(visitor);
		ctx.setVisitor(visitor);

		if(stmt instanceof SQLSelectStatement){
			SQLSelectQuery query = ((SQLSelectStatement) stmt).getSelect().getQuery();
			if(query instanceof MySqlSelectQueryBlock){
				if(((MySqlSelectQueryBlock)query).isForUpdate()){
					rrs.setSelectForUpdate(true);
				}
			}
		}

		List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
		if(visitor.hasOrCondition()) {//包含or语句
			//TODO
			//根据or拆分
			mergedConditionList = visitor.splitConditions();
		} else {//不包含OR语句
			mergedConditionList.add(visitor.getConditions());
		}
		
		if(visitor.isHasChange()){	// 在解析的过程中子查询被改写了.需要更新ctx.
			ctx.setSql(stmt.toString());
			rrs.setStatement(ctx.getSql());
		}
		
		if(visitor.getTables() != null) {
			ctx.addTables(visitor.getTables());
		}
		ctx.setRouteCalculateUnits(this.buildRouteCalculateUnits(visitor, mergedConditionList));
	}
	
	private List<RouteCalculateUnit> buildRouteCalculateUnits(SchemaStatVisitor visitor, List<List<Condition>> conditionList) {
		List<RouteCalculateUnit> retList = new ArrayList<RouteCalculateUnit>();
		//遍历condition ，找分片字段
		for(int i = 0; i < conditionList.size(); i++) {
			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			for(Condition condition : conditionList.get(i)) {
				List<Object> values = condition.getValues();
				if(values.size() == 0) {
					continue;  
				}
				if(checkConditionValues(values)) {
					String columnName = StringUtil.removeBackquote(condition.getColumn().getName());
					String tableName = StringUtil.removeBackquote(condition.getColumn().getTable());

					if(visitor.getTables() != null
							&& !visitor.containsTable(tableName)) {
						//子查询的别名条件忽略掉,不参数路由计算，否则后面找不到表
						String upColumnName = StringUtil.removeBackquote(condition.getColumn().getName().toUpperCase());
						String upTableName = StringUtil.removeBackquote(condition.getColumn().getTable().toUpperCase());

						if(!visitor.containsTable(upTableName)){
							continue;
						}
					}
					
					String operator = condition.getOperator();
					
					//只处理between ,in和=3中操作符
					if(operator.equalsIgnoreCase("between")) {
						RangeValue rv = new RangeValue(values.get(0), values.get(1), RangeValue.EE);
						routeCalculateUnit.addShardingExpr(tableName.toUpperCase(), columnName, rv);
					} else if(operator.equals("=") || operator.equalsIgnoreCase("in")){
						//只处理=号和in操作符,其他忽略
						routeCalculateUnit.addShardingExpr(tableName.toUpperCase(), columnName, values.toArray());
					}
				}
			}
			retList.add(routeCalculateUnit);
		}
		return retList;
	}
	
	private boolean checkConditionValues(List<Object> values) {
		for(Object value : values) {
			if(value != null && !value.toString().equals("")) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 获取解析到的信息
	 * @return
	 */
	@Override
	public DruidShardingParseInfo getCtx() {
		return ctx;
	}
}
