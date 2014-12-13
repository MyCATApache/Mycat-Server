package org.opencloudb.route.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.mpp.ColumnRoutePair;
import org.opencloudb.parser.druid.DruidParser;
import org.opencloudb.parser.druid.DruidParserFactory;
import org.opencloudb.parser.druid.DruidShardingParseInfo;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.parser.ServerParse;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

public class DruidMysqlRouteStrategy extends AbstractRouteStrategy {
	private static final Logger LOGGER = Logger.getLogger(DruidMysqlRouteStrategy.class);
	
	@Override
	public RouteResultset routeNormalSqlWithAST(SchemaConfig schema,
			String stmt, RouteResultset rrs, String charset,
			LayerCachePool cachePool) throws SQLNonTransientException {
		MySqlStatementParser parser = new MySqlStatementParser(stmt);
		
		SQLStatement statement = parser.parseStatement();
		
		//检验unsupported statement
		checkUnSupportedStatement(statement);
			
		DruidParser druidParser = DruidParserFactory.create(statement);
		druidParser.parser(schema, rrs, statement);
		
		//DruidParser解析过程中已完成了路由的直接返回
		if(rrs.isFinishedRoute()) {
			return rrs;
		}
		
		rrs.setStatement(druidParser.getCtx().getSql());
		//没有from的的select语句或其他
		if(druidParser.getCtx().getTables().size() == 0) {
			return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(),druidParser.getCtx().getSql());
		}

		return tryRouteForTables(schema, druidParser.getCtx(), druidParser.getCtx().getTables(), rrs, isSelect(statement));
	}
	
	private boolean isSelect(SQLStatement statement) {
		if(statement instanceof SQLSelectStatement) {
			return true;
		}
		return false;
	}
	
	/**
	 * 检验不支持的SQLStatement类型 ：不支持的类型直接抛SQLSyntaxErrorException异常
	 * @param statement
	 * @throws SQLSyntaxErrorException
	 */
	private void checkUnSupportedStatement(SQLStatement statement) throws SQLSyntaxErrorException {
		if(statement instanceof MySqlReplaceStatement) {
			throw new SQLSyntaxErrorException(" ReplaceStatement can't be supported,use insert into ...on duplicate key update... instead ");
		}
	}
	
	/**
	 * 
	 */
	@Override
	public RouteResultset analyseShowSQL(SchemaConfig schema,
			RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
		String upStmt = stmt.toUpperCase();
		int tabInd = upStmt.indexOf(" TABLES");
		if (tabInd > 0) {// show tables
			int[] nextPost = RouterUtil.getSpecPos(upStmt, 0);
			if (nextPost[0] > 0) {// remove db info
				int end = RouterUtil.getSpecEndPos(upStmt, tabInd);
				if (upStmt.indexOf(" FULL") > 0) {
					stmt = "SHOW FULL TABLES" + stmt.substring(end);
				} else {
					stmt = "SHOW TABLES" + stmt.substring(end);
				}
			}
			return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
		}
		// show index or column
		int[] indx = RouterUtil.getSpecPos(upStmt, 0);
		if (indx[0] > 0) {
			// has table
			int[] repPos = { indx[0] + indx[1], 0 };
			String tableName = RouterUtil.getTableName(stmt, repPos);
			// IN DB pattern
			int[] indx2 = RouterUtil.getSpecPos(upStmt, indx[0] + indx[1] + 1);
			if (indx2[0] > 0) {// find LIKE OR WHERE
				repPos[1] = RouterUtil.getSpecEndPos(upStmt, indx2[0] + indx2[1]);

			}
			stmt = stmt.substring(0, indx[0]) + " FROM " + tableName
					+ stmt.substring(repPos[1]);
			RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
			return rrs;

		}
		// show create table tableName
		int[] createTabInd = RouterUtil.getCreateTablePos(upStmt, 0);
		if (createTabInd[0] > 0) {
			int tableNameIndex = createTabInd[0] + createTabInd[1];
			if (upStmt.length() > tableNameIndex) {
				String tableName = stmt.substring(tableNameIndex).trim();
				int ind2 = tableName.indexOf('.');
				if (ind2 > 0) {
					tableName = tableName.substring(ind2 + 1);
				}
				RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
				return rrs;
			}
		}

		return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}
	
	/**
	 * 单表路由
	 * @param schema
	 * @param ctx
	 * @param tableName
	 * @param rrs
	 * @param isSelect
	 * @return
	 * @throws SQLNonTransientException
	 */
	private static RouteResultset tryRouteForOneTable(SchemaConfig schema, DruidShardingParseInfo ctx, String tableName, RouteResultset rrs,
			boolean isSelect) throws SQLNonTransientException {
		TableConfig tc = schema.getTables().get(tableName);
		if(tc == null) {
			String msg = "can't find table define in schema "
					+ tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		if(tc.isGlobalTable()) {//全局表
			if(isSelect) {
				return RouterUtil.routeToSingleNode(rrs, tc.getRandomDataNode(),ctx.getSql());
			} else {
				return RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql());
			}
		} else {//单表或者分库表
			if(tc.getPartitionColumn() == null) {//单表
				return RouterUtil.routeToSingleNode(rrs, tc.getDataNodes().get(0),ctx.getSql());
			} else {
				//每个表对应的路由映射
				Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
				findRouteForShardingTables(schema, ctx, tablesRouteMap);
				if(tablesRouteMap.get(tableName) == null) {
					return RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql());
				} else {
					return RouterUtil.routeToMultiNode(false, rrs, tablesRouteMap.get(tableName), ctx.getSql());
				}
			}
		}
	}
	
	/**
	 * 多表路由
	 * @param schema
	 * @param ctx
	 * @param tables
	 * @param rrs
	 * @param isSelect
	 * @return
	 * @throws SQLNonTransientException
	 */
	private static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx, List<String> tables, RouteResultset rrs,
			boolean isSelect) throws SQLNonTransientException {
		//只有一个表的
		if(tables.size() == 1) {
			tryRouteForOneTable(schema, ctx, tables.get(0), rrs, isSelect);
		}
		
		Set<String> retNodesSet = new HashSet<String>();
		//每个表对应的路由映射
		Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
		
		//分库解析信息不为空
		if(ctx != null) {
			//为分库表找路由
			findRouteForShardingTables(schema, ctx, tablesRouteMap);
		}
		
		
		//为全局表和单库表找路由
		for(String tableName : tables) {
			TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
			if(tableConfig == null) {
				String msg = "can't find table define in schema "+ tableName + " schema:" + schema.getName();
				LOGGER.warn(msg);
				throw new SQLNonTransientException(msg);
			}
			if(tableConfig.isGlobalTable()) {//全局表
				if(tablesRouteMap.get(tableName) == null) {
					tablesRouteMap.put(tableName, new HashSet<String>());
				}
				tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
			} else if(tablesRouteMap.get(tableName) == null) { //余下的表都是单库表
				tablesRouteMap.put(tableName, new HashSet<String>());
				tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
			}
		}
		
		//所有表路由汇总分析,求交集
		if(tables.size() == 1) {
			TableConfig tableConfig = schema.getTables().get(tables.get(0).toUpperCase());
			if(tableConfig.isGlobalTable()) {
				if(isSelect) {
					return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(),ctx.getSql());
				}
			}
		}
		
		boolean isFirstAdd = true;
		for(Map.Entry<String, Set<String>> entry : tablesRouteMap.entrySet()) {
			if(entry.getValue() == null || entry.getValue().size() == 0) {
				throw new SQLNonTransientException("parent key can't find any valid datanode ");
			} else {
				if(isFirstAdd) {
					retNodesSet.addAll(entry.getValue());
					isFirstAdd = false;
				} else {
					retNodesSet.retainAll(entry.getValue());
					if(retNodesSet.size() == 0) {//两个表的路由无交集
						String errMsg = "invalid route in sql, multi tables found but datanode has no intersection "
								+ " sql:" + ctx.getSql();
						LOGGER.warn(errMsg);
						throw new SQLNonTransientException(errMsg);
					}
				}
			}
		}
		
		if(retNodesSet != null && retNodesSet.size() > 0) {
			RouterUtil.routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql());
		}
		return rrs;
		
	}

	/**
	 * 处理分库表路由
	 * @param schema
	 * @param ctx
	 * @param tablesRouteMap
	 * @throws SQLNonTransientException
	 */
	private static void findRouteForShardingTables(SchemaConfig schema,
			DruidShardingParseInfo ctx, Map<String, Set<String>> tablesRouteMap)
			throws SQLNonTransientException {
		Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = ctx.getTablesAndConditions();
		//为分库表找路由
		for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
			String tableName = entry.getKey().toUpperCase();
			TableConfig tableConfig = schema.getTables().get(tableName);
			//全局表或者不分库的表略过（全局表后面再计算）
			if(tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
				continue;
			} else {//需要分库的表
				Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
				//是否找到了拆分字段
				boolean isFoundShardingCol = false;
				for(Map.Entry<String, Set<ColumnRoutePair>> condition : columnsMap.entrySet()) {
					
					String colName = condition.getKey();
					//条件字段是拆分字段
					if(colName.equals(tableConfig.getPartitionColumn())) {
						isFoundShardingCol = true;
						Set<ColumnRoutePair> columnPairs = condition.getValue();
						
						for(ColumnRoutePair pair : columnPairs) {
							if(pair.colValue != null) {
								Integer nodeIndex = tableConfig.getRule().getRuleAlgorithm().calculate(pair.colValue);
								if(nodeIndex == null) {
									String msg = "can't find any valid datanode :" + tableConfig.getName() 
											+ " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
									LOGGER.warn(msg);
									throw new SQLNonTransientException(msg);
								}
								String node = tableConfig.getDataNodes().get(nodeIndex);
								if(node != null) {
									if(tablesRouteMap.get(tableName) == null) {
										tablesRouteMap.put(tableName, new HashSet<String>());
									}
									tablesRouteMap.get(tableName).add(node);
								}
							}
							if(pair.rangeValue != null) {
								Integer[] nodeIndexs = tableConfig.getRule().getRuleAlgorithm()
										.calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
								for(Integer idx : nodeIndexs) {
									String node = tableConfig.getDataNodes().get(idx);
									if(node != null) {
										if(tablesRouteMap.get(tableName) == null) {
											tablesRouteMap.put(tableName, new HashSet<String>());
										}
										tablesRouteMap.get(tableName).add(node);
									}
								}
							}
						}
						
						
					} else {//条件字段不是拆分字段,略过
						continue;
						
					}
					
				}
				//找到了拆分字段，但是一个节点都没找到
				if(isFoundShardingCol) {
					if(tablesRouteMap.get(tableName).size() == 0) {
						throw new SQLNonTransientException("parent key can't find any valid datanode ");
					}
					
				} else {//没找到拆分字段，该表的所有节点都路由
					if(tablesRouteMap.get(tableName) == null) {
						tablesRouteMap.put(tableName, new HashSet<String>());
					}
					tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
				}
			}

		}
	}

	

	
	public RouteResultset routeSystemInfo(SchemaConfig schema, int sqlType,
			String stmt, RouteResultset rrs) throws SQLSyntaxErrorException {
		switch(sqlType){
		case ServerParse.SHOW:// if origSQL is like show tables
			return analyseShowSQL(schema, rrs, stmt);
		case ServerParse.SELECT://if origSQL is like select @@
			if(stmt.contains("@@")){
				return analyseDoubleAtSgin(schema, rrs, stmt);
			}
			break;
		case ServerParse.DESCRIBE:// if origSQL is meta SQL, such as describe table
			int ind = stmt.indexOf(' ');
			return analyseDescrSQL(schema, rrs, stmt, ind + 1);
		}
		return null;
	}
	
	/**
	 * 对Desc语句进行分析 返回数据路由集合
	 * 
	 * @param schema
	 *            数据库名
	 * @param rrs
	 *            数据路由集合
	 * @param stmt
	 *            执行语句
	 * @param ind
	 *            第一个' '的位置
	 * @return RouteResultset(数据路由集合)
	 * @author mycat
	 */
	private static RouteResultset analyseDescrSQL(SchemaConfig schema,
			RouteResultset rrs, String stmt, int ind) {
		int[] repPos = { ind, 0 };
		String tableName = RouterUtil.getTableName(stmt, repPos);
		stmt = stmt.substring(0, ind) + tableName + stmt.substring(repPos[1]);
		RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
		return rrs;
	}
	
	/**
	 * 根据执行语句判断数据路由
	 * 
	 * @param schema
	 *            数据库名
	 * @param rrs
	 *            数据路由集合
	 * @param stmt
	 *            执行sql
	 * @return RouteResultset数据路由集合
	 * @throws SQLSyntaxErrorException
	 * @author mycat
	 */
	private RouteResultset analyseDoubleAtSgin(SchemaConfig schema,
			RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
		String upStmt = stmt.toUpperCase();

		int atSginInd = upStmt.indexOf(" @@");
		if (atSginInd > 0) {
			return RouterUtil.routeToMultiNode(false, rrs,
					schema.getMetaDataNodes(), stmt);
		}

		return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}
}
