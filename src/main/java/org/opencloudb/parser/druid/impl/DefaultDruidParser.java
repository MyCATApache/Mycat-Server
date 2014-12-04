package org.opencloudb.parser.druid.impl;

import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mpp.RangeValue;
import org.opencloudb.parser.druid.DruidParser;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.parser.druid.MycatSchemaStatVisitor;
import org.opencloudb.route.RouteResultset;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.stat.TableStat.Condition;

/**
 * 对SQLStatement解析
 * 主要通过visitor解析和statement解析：有些类型的SQLStatement通过visitor解析足够了，
 *  有些只能通过statement解析才能得到所有信息
 *  有些需要通过两种方式解析才能得到完整信息
 * @author wang.dw
 *
 */
public class DefaultDruidParser implements DruidParser {
	protected static final Logger LOGGER = Logger.getLogger(DefaultDruidParser.class);
	/**
	 * 解析得到的结果
	 */
	protected DruidShardingParseInfo ctx;
	
	private Map<String,String> tableAliasMap = new HashMap<String,String>();

	private List<Condition> conditions = new ArrayList<Condition>();
	
	public Map<String, String> getTableAliasMap() {
		return tableAliasMap;
	}

	public List<Condition> getConditions() {
		return conditions;
	}
	
	/**
	 * 使用MycatSchemaStatVisitor解析,得到tables、tableAliasMap、conditions等
	 * @param schema
	 * @param stmt
	 */
	public void parser(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		ctx = new DruidShardingParseInfo();
		//通过visitor解析
		visitorParse(rrs,stmt);
		//通过Statement解析
		statementParse(schema, rrs, stmt);
		
		//改写sql：如insert语句主键自增长的可以
		changeSql(schema, rrs, stmt);
		
		ctx.setSql(stmt.toString());
	}
	
	/**
	 * 子类可覆盖（如果visitorParse解析得不到表名、字段等信息的，就通过覆盖该方法来解析）
	 * 子类覆盖该方法一般是将SQLStatement转型后再解析（如转型为MySqlInsertStatement）
	 */
	@Override
	public void statementParse(SchemaConfig schema, RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException {
		
	}
	
	/**
	 * 改写sql：如insert是
	 */
	@Override
	public void changeSql(SchemaConfig schema, RouteResultset rrs,
			SQLStatement stmt) throws SQLNonTransientException {
		
	}

	/**
	 * 子类可覆盖（如果该方法解析得不到表名、字段等信息的，就覆盖该方法，覆盖成空方法，然后通过statementPparse去解析）
	 * 通过visitor解析：有些类型的Statement通过visitor解析得不到表名、
	 * @param stmt
	 */
	@Override
	public void visitorParse(RouteResultset rrs, SQLStatement stmt) throws SQLNonTransientException{
		MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();
		stmt.accept(visitor);
		
		if(visitor.getAliasMap() != null) {
			for(Map.Entry<String, String> entry : visitor.getAliasMap().entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				if(key != null && key.indexOf("`") >= 0) {
					key = key.replaceAll("`", "");
				}
				if(value != null && value.indexOf("`") >= 0) {
					value = value.replaceAll("`", "");
				}
				//表名前面带database的，去掉
				if(key != null) {
					int pos = key.indexOf(".");
					if(pos> 0) {
						key = key.substring(pos + 1);
					}
				}
				
				if(key.equals(value)) {
					ctx.addTable(key.toUpperCase());
				} else {
					tableAliasMap.put(key, value);
				}
			}
			ctx.setTableAliasMap(tableAliasMap);
		}

		
		conditions = visitor.getConditions();
		
		//遍历condition ，找分片字段
		for(Condition condition : conditions) {
			List<Object> values = condition.getValues();
			if(values.size() == 0) {
				break;
			}
			if(checkConditionValues(values)) {
				String columnName = removeBackquote(condition.getColumn().getName().toUpperCase());
				String tableName = removeBackquote(condition.getColumn().getTable().toUpperCase());
				
				String operator = condition.getOperator();
				
				
				//between \ in 、>= > = < =< ,in和=是一样的处理逻辑
				if(operator.equals("between")) {
					RangeValue rv = new RangeValue(values.get(0), values.get(1), RangeValue.EE);
					ctx.addShardingExpr(tableName.toUpperCase(), columnName, rv);
				} else {
					ctx.addShardingExpr(tableName.toUpperCase(), columnName, values.toArray());
				}
			}
		}
	}
	
	private boolean checkConditionValues(List<Object> values) {
		for(Object value : values) {
			if(value != null && !value.toString().equals("")) {
				return true;
			}
		}
		return false;
	}
	
	public DruidShardingParseInfo getCtx() {
		return ctx;
	}
	
	/**
	 * 移除`符号
	 * @param str
	 * @return
	 */
	public String removeBackquote(String str){
		//删除名字中的`tablename`和'value'
		if (str.length() > 0) {
			StringBuilder sb = new StringBuilder(str);
			if (sb.charAt(0) == '`'||sb.charAt(0) == '\'') {
				sb.deleteCharAt(0);
			}
			if (sb.charAt(sb.length() - 1) == '`'||sb.charAt(sb.length() - 1) == '\'') {
				sb.deleteCharAt(sb.length() - 1);
			}
			return sb.toString();
		}
		return "";
	}
}
