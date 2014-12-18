package org.opencloudb.route.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
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
		druidParser.parser(schema, rrs, statement, stmt);
		
		//DruidParser解析过程中已完成了路由的直接返回
		if(rrs.isFinishedRoute()) {
			return rrs;
		}
		
//		rrs.setStatement(druidParser.getCtx().getSql());
		//没有from的的select语句或其他
		if(druidParser.getCtx().getTables().size() == 0) {
			return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(),druidParser.getCtx().getSql());
		}

		return tryRouteForTables(schema, druidParser.getCtx(), rrs, isSelect(statement),cachePool);
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
	 * 
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
			boolean isSelect, LayerCachePool cachePool) throws SQLNonTransientException {
		TableConfig tc = schema.getTables().get(tableName);
		if(tc == null) {
			String msg = "can't find table define in schema "
					+ tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		if(tc.isGlobalTable()) {//全局表
			if(isSelect) {
				// global select ,not cache route result
				rrs.setCacheAble(false);
				return RouterUtil.routeToSingleNode(rrs, tc.getRandomDataNode(),ctx.getSql());
			} else {
				return RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql());
			}
		} else {//单表或者分库表
			if (!checkRuleRequired(schema, ctx, tc)) {
				throw new IllegalArgumentException("route rule for table "
						+ tc.getName() + " is required: " + ctx.getSql());

			}
			if(tc.getPartitionColumn() == null && !tc.isSecondLevel()) {//单表且不是childTable
				return RouterUtil.routeToSingleNode(rrs, tc.getDataNodes().get(0),ctx.getSql());
			} else {
				//每个表对应的路由映射
				Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
				if(ctx.getTablesAndConditions() != null && ctx.getTablesAndConditions().size() > 0) {
					findRouteWithcConditionsForTables(schema, rrs, ctx.getTablesAndConditions(), tablesRouteMap, ctx.getSql(),cachePool,isSelect);
					if(rrs.isFinishedRoute()) {
						return rrs;
					}
				}
				
				if(tablesRouteMap.get(tableName) == null) {
					return RouterUtil.routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql());
				} else {
//					boolean isCache = rrs.isCacheAble();
//					if(tablesRouteMap.get(tableName).size() > 1) {
//						
//					}
					return RouterUtil.routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName), ctx.getSql());
				}
			}
		}
	}
	
	/**
	 * 
	 * @param schema
	 * @param ctx
	 * @param tc
	 * @return true表示校验通过，false表示检验不通过
	 */
	private static boolean checkRuleRequired(SchemaConfig schema, DruidShardingParseInfo ctx, TableConfig tc) {
		if(!tc.isRuleRequired()) {
			return true;
		}
		boolean hasRequiredValue = false;
		String tableName = tc.getName();
		if(ctx.getTablesAndConditions().get(tableName) == null || ctx.getTablesAndConditions().get(tableName).size() == 0) {
			hasRequiredValue = false;
		} else {
			for(Map.Entry<String, Set<ColumnRoutePair>> condition : ctx.getTablesAndConditions().get(tableName).entrySet()) {
				
				String colName = condition.getKey();
				//条件字段是拆分字段
				if(colName.equals(tc.getPartitionColumn())) {
					hasRequiredValue = true;
					break;
				}
			}
		}
		return hasRequiredValue;
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
	private static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx, RouteResultset rrs,
			boolean isSelect, LayerCachePool cachePool) throws SQLNonTransientException {
		if(schema.isNoSharding()) {
			return RouterUtil.routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
		}
		List<String> tables = ctx.getTables();
		//只有一个表的
		if(tables.size() == 1) {
			return tryRouteForOneTable(schema, ctx, tables.get(0), rrs, isSelect, cachePool);
		}
		
		Set<String> retNodesSet = new HashSet<String>();
		//每个表对应的路由映射
		Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
		
		//分库解析信息不为空
		Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = ctx.getTablesAndConditions();
		if(tablesAndConditions != null && tablesAndConditions.size() > 0) {
			//为分库表找路由
			findRouteWithcConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, ctx.getSql(), cachePool, isSelect);
			if(rrs.isFinishedRoute()) {
				return rrs;
			}
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
			if(retNodesSet.size() > 1 && isAllGlobalTable(ctx, schema)) {
				// mulit routes ,not cache route result
				rrs.setCacheAble(false);
				RouterUtil.routeToSingleNode(rrs, retNodesSet.iterator().next(), ctx.getSql());
			} else {
				RouterUtil.routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql());
			}
			
		}
		return rrs;
		
	}
	
	private static boolean isAllGlobalTable(DruidShardingParseInfo ctx, SchemaConfig schema) {
		boolean isAllGlobal = false;
		for(String table : ctx.getTables()) {
			if(schema.getTables().get(table).isGlobalTable()) {
				isAllGlobal = true;
			} else {
				return false;
			}
		}
		return isAllGlobal;
	}

	/**
	 * 处理分库表路由
	 * @param schema
	 * @param tablesAndConditions
	 * @param tablesRouteMap
	 * @throws SQLNonTransientException
	 */
	private static void findRouteWithcConditionsForTables(SchemaConfig schema, RouteResultset rrs,
			Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions, 
			Map<String, Set<String>> tablesRouteMap, String sql, LayerCachePool cachePool, boolean isSelect)
			throws SQLNonTransientException {
		//为分库表找路由
		for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
			String tableName = entry.getKey().toUpperCase();
			TableConfig tableConfig = schema.getTables().get(tableName);
			if(tableConfig == null) {
				String msg = "can't find table define in schema "
						+ tableName + " schema:" + schema.getName();
				LOGGER.warn(msg);
				throw new SQLNonTransientException(msg);
			}
			//全局表或者不分库的表略过（全局表后面再计算）
			if(tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
				continue;
			} else {//非全局表：分库表、childTable、其他
				Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
				String joinKey = tableConfig.getJoinKey();
				String partionCol = tableConfig.getPartitionColumn();
				String primaryKey = tableConfig.getPrimaryKey();
				boolean isPartitionFound = false;
				boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
				if(entry.getValue().get(primaryKey) != null && entry.getValue().size() == 1) {//主键查找
					// try by primary key if found in cache
					Set<ColumnRoutePair> primaryKeyPairs = entry.getValue().get(primaryKey);
					if (primaryKeyPairs != null) {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("try to find cache by primary key ");
						}
						boolean cache = rrs.isCacheAble();
						Set<String> dataNodes = new HashSet<String>(
								primaryKeyPairs.size());
						String tableKey = schema.getName() + '_' + tableName;
						boolean allFound = true;
						for (ColumnRoutePair pair : primaryKeyPairs) {
							String cacheKey = pair.colValue;
							String dataNode = (String) cachePool.get(tableKey, cacheKey);
							if (dataNode == null) {
								allFound = false;
								break;
							} else {
								if(tablesRouteMap.get(tableName) == null) {
									tablesRouteMap.put(tableName, new HashSet<String>());
								}
								tablesRouteMap.get(tableName).add(dataNode);
//								rrs.setLimitSize(-1);
							}
						}
						if (!allFound) {
							// need cache primary key ->datanode relation
							if (isSelect && tableConfig.getPrimaryKey() != null) {
								rrs.setPrimaryKey(tableKey + '.' + tableConfig.getPrimaryKey());
							}
						}
					}
				} else if (isFoundPartitionValue) {//分库表
					Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
					if(partitionValue == null || partitionValue.size() == 0) {
						if(tablesRouteMap.get(tableName) == null) {
							tablesRouteMap.put(tableName, new HashSet<String>());
						}
						tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
					} else {
						for(ColumnRoutePair pair : partitionValue) {
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
					}
				} else if(joinKey != null) {//childTable  (如果是select 语句的父子表join)之前要找到root table,将childTable移除,只留下root table
					Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);

					Set<String> dataNodeSet = RouterUtil.ruleCalculate(
							tableConfig.getParentTC(), joinKeyValue);
					if (dataNodeSet.isEmpty()) {
						throw new SQLNonTransientException(
								"parent key can't find any valid datanode ");
					}
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  "
								+ Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
					}
					if (dataNodeSet.size() > 1) {
						RouterUtil.routeToMultiNode(rrs.isCacheAble(), rrs, schema.getAllDataNodes(), sql);
						return;
					} else {
						rrs.setCacheAble(true);
						RouterUtil.routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
						return;
					}
				
				} else {
					//没找到拆分字段，该表的所有节点都路由
					if(tablesRouteMap.get(tableName) == null) {
						tablesRouteMap.put(tableName, new HashSet<String>());
					}
					tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
				}
			}
		}
	}
	
//	/**
//	 * 为一个表进行条件路由
//	 * @param schema
//	 * @param tablesAndConditions
//	 * @param tablesRouteMap
//	 * @throws SQLNonTransientException
//	 */
//	private static RouteResultset findRouteWithcConditionsForOneTable(SchemaConfig schema, RouteResultset rrs,
//		Map<String, Set<ColumnRoutePair>> conditions, String tableName, String sql) throws SQLNonTransientException {
//		boolean cache = rrs.isCacheAble();
//	    //为分库表找路由
//		tableName = tableName.toUpperCase();
//		TableConfig tableConfig = schema.getTables().get(tableName);
//		//全局表或者不分库的表略过（全局表后面再计算）
//		if(tableConfig.isGlobalTable()) {
//			return null;
//		} else {//非全局表
//			Set<String> routeSet = new HashSet<String>();
//			String joinKey = tableConfig.getJoinKey();
//			for(Map.Entry<String, Set<ColumnRoutePair>> condition : conditions.entrySet()) {
//				String colName = condition.getKey();
//				//条件字段是拆分字段
//				if(colName.equals(tableConfig.getPartitionColumn())) {
//					Set<ColumnRoutePair> columnPairs = condition.getValue();
//					
//					for(ColumnRoutePair pair : columnPairs) {
//						if(pair.colValue != null) {
//							Integer nodeIndex = tableConfig.getRule().getRuleAlgorithm().calculate(pair.colValue);
//							if(nodeIndex == null) {
//								String msg = "can't find any valid datanode :" + tableConfig.getName() 
//										+ " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
//								LOGGER.warn(msg);
//								throw new SQLNonTransientException(msg);
//							}
//							String node = tableConfig.getDataNodes().get(nodeIndex);
//							if(node != null) {//找到一个路由节点
//								routeSet.add(node);
//							}
//						}
//						if(pair.rangeValue != null) {
//							Integer[] nodeIndexs = tableConfig.getRule().getRuleAlgorithm()
//									.calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
//							for(Integer idx : nodeIndexs) {
//								String node = tableConfig.getDataNodes().get(idx);
//								if(node != null) {//找到一个路由节点
//									routeSet.add(node);
//								}
//							}
//						}
//					}
//				} else if(joinKey != null && joinKey.equals(colName)) {
//					Set<String> dataNodeSet = RouterUtil.ruleCalculate(
//							tableConfig.getParentTC(), condition.getValue());
//					if (dataNodeSet.isEmpty()) {
//						throw new SQLNonTransientException(
//								"parent key can't find any valid datanode ");
//					}
//					if (LOGGER.isDebugEnabled()) {
//						LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  "
//								+ Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
//					}
//					if (dataNodeSet.size() > 1) {
//						return RouterUtil.routeToMultiNode(rrs.isCacheAble(), rrs, schema.getAllDataNodes(), sql);
//					} else {
//						rrs.setCacheAble(true);
//						return RouterUtil.routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
//					}
//				} else {//条件字段不是拆分字段也不是join字段,略过
//					continue;
//					
//				}
//			}
//			return RouterUtil.routeToMultiNode(cache, rrs, routeSet, sql);
//			
//		}
//
//	}

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