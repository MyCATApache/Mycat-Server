package io.mycat.route.impl;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.function.SlotFunction;
import io.mycat.route.parser.util.ParseUtil;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.google.common.base.Strings;

import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.DruidParserFactory;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.route.parser.druid.MycatStatementParser;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.parser.ServerParse;

public class DruidMycatRouteStrategy extends AbstractRouteStrategy {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(DruidMycatRouteStrategy.class);
	
	@Override
	public RouteResultset routeNormalSqlWithAST(SchemaConfig schema,
			String stmt, RouteResultset rrs, String charset,
			LayerCachePool cachePool) throws SQLNonTransientException {
		
		/**
		 *  只有mysql时只支持mysql语法
		 */
		SQLStatementParser parser = null;
		if (schema.isNeedSupportMultiDBType()) {
			parser = new MycatStatementParser(stmt);
		} else {
			parser = new MySqlStatementParser(stmt); 
		}

		MycatSchemaStatVisitor visitor = null;
		SQLStatement statement;
		
		/**
		 * 解析出现问题统一抛SQL语法错误
		 */
		try {
			statement = parser.parseStatement();
            visitor = new MycatSchemaStatVisitor();
		} catch (Exception t) {
	        LOGGER.error("DruidMycatRouteStrategyError", t);
			throw new SQLSyntaxErrorException(t);
		}

		/**
		 * 检验unsupported statement
		 */
		checkUnSupportedStatement(statement);


		DruidParser druidParser = DruidParserFactory.create(schema, statement, visitor);
		druidParser.parser(schema, rrs, statement, stmt,cachePool,visitor);
		DruidShardingParseInfo ctx=  druidParser.getCtx() ;
		rrs.setTables(ctx.getTables());
		/**
		 * DruidParser 解析过程中已完成了路由的直接返回
		 */
		if ( rrs.isFinishedRoute() ) {
			return rrs;
		}
		
		/**
		 * 没有from的select语句或其他
		 */
        if((ctx.getTables() == null || ctx.getTables().size() == 0)&&(ctx.getTableAliasMap()==null||ctx.getTableAliasMap().isEmpty()))
        {
		    return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), druidParser.getCtx().getSql());
		}

		if(druidParser.getCtx().getRouteCalculateUnits().size() == 0) {
			RouteCalculateUnit routeCalculateUnit = new RouteCalculateUnit();
			druidParser.getCtx().addRouteCalculateUnit(routeCalculateUnit);
		}
		
		SortedSet<RouteResultsetNode> nodeSet = new TreeSet<RouteResultsetNode>();
		for(RouteCalculateUnit unit: druidParser.getCtx().getRouteCalculateUnits()) {
			RouteResultset rrsTmp = RouterUtil.tryRouteForTables(schema, druidParser.getCtx(), unit, rrs, isSelect(statement), cachePool);
			if(rrsTmp != null) {
				for(RouteResultsetNode node :rrsTmp.getNodes()) {
					nodeSet.add(node);
				}
			}
		}
		
		RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSet.size()];
		int i = 0;
		for (RouteResultsetNode aNodeSet : nodeSet) {
			nodes[i] = aNodeSet;
			  if(statement instanceof MySqlInsertStatement &&ctx.getTables().size()==1&&schema.getTables().containsKey(ctx.getTables().get(0))) {
				  RuleConfig rule = schema.getTables().get(ctx.getTables().get(0)).getRule();
				  if(rule!=null&&  rule.getRuleAlgorithm() instanceof SlotFunction){
					 aNodeSet.setStatement(ParseUtil.changeInsertAddSlot(aNodeSet.getStatement(),aNodeSet.getSlot()));
				  }
			  }
			i++;
		}		
		rrs.setNodes(nodes);		
		
		//分表
		/**
		 *  subTables="t_order$1-2,t_order3"
		 *目前分表 1.6 开始支持 幵丏 dataNode 在分表条件下只能配置一个，分表条件下不支持join。
		 */
		if(rrs.isDistTable()){
			return this.routeDisTable(statement,rrs);
		}
		
		return rrs;
	}



	
	private SQLExprTableSource getDisTable(SQLTableSource tableSource,RouteResultsetNode node) throws SQLSyntaxErrorException{
		if(node.getSubTableName()==null){
			String msg = " sub table not exists for " + node.getName() + " on " + tableSource;
			LOGGER.error("DruidMycatRouteStrategyError " + msg);
			throw new SQLSyntaxErrorException(msg);
		}
		
		SQLIdentifierExpr sqlIdentifierExpr = new SQLIdentifierExpr();
		sqlIdentifierExpr.setParent(tableSource.getParent());
		sqlIdentifierExpr.setName(node.getSubTableName());
		SQLExprTableSource from2 = new SQLExprTableSource(sqlIdentifierExpr);
		return from2;
	}
	
	private RouteResultset routeDisTable(SQLStatement statement, RouteResultset rrs) throws SQLSyntaxErrorException{
		SQLTableSource tableSource = null;
		if(statement instanceof SQLInsertStatement) {
			SQLInsertStatement insertStatement = (SQLInsertStatement) statement;
			tableSource = insertStatement.getTableSource();
			for (RouteResultsetNode node : rrs.getNodes()) {
				SQLExprTableSource from2 = getDisTable(tableSource, node);
				insertStatement.setTableSource(from2);
				node.setStatement(insertStatement.toString());
	        }
		}
		if(statement instanceof SQLDeleteStatement) {
			SQLDeleteStatement deleteStatement = (SQLDeleteStatement) statement;
			tableSource = deleteStatement.getTableSource();
			for (RouteResultsetNode node : rrs.getNodes()) {
				SQLExprTableSource from2 = getDisTable(tableSource, node);
				deleteStatement.setTableSource(from2);
				node.setStatement(deleteStatement.toString());
	        }
		}
		if(statement instanceof SQLUpdateStatement) {
			SQLUpdateStatement updateStatement = (SQLUpdateStatement) statement;
			tableSource = updateStatement.getTableSource();
			for (RouteResultsetNode node : rrs.getNodes()) {
				SQLExprTableSource from2 = getDisTable(tableSource, node);
				updateStatement.setTableSource(from2);
				node.setStatement(updateStatement.toString());
	        }
		}
		
		return rrs;
	}

	/**
	 * SELECT 语句
	 */
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
		//不支持replace语句
		if(statement instanceof MySqlReplaceStatement) {
			throw new SQLSyntaxErrorException(" ReplaceStatement can't be supported,use insert into ...on duplicate key update... instead ");
		}
	}
	
	/**
	 * 分析 SHOW SQL
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
          String defaultNode=  schema.getDataNode();
            if(!Strings.isNullOrEmpty(defaultNode))
            {
             return    RouterUtil.routeToSingleNode(rrs, defaultNode, stmt);
            }
			return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
		}
		
		/**
		 *  show index or column
		 */
		int[] indx = RouterUtil.getSpecPos(upStmt, 0);
		if (indx[0] > 0) {
			/**
			 *  has table
			 */
			int[] repPos = { indx[0] + indx[1], 0 };
			String tableName = RouterUtil.getShowTableName(stmt, repPos);
			/**
			 *  IN DB pattern
			 */
			int[] indx2 = RouterUtil.getSpecPos(upStmt, indx[0] + indx[1] + 1);
			if (indx2[0] > 0) {// find LIKE OR WHERE
				repPos[1] = RouterUtil.getSpecEndPos(upStmt, indx2[0] + indx2[1]);

			}
			stmt = stmt.substring(0, indx[0]) + " FROM " + tableName + stmt.substring(repPos[1]);
			RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
			return rrs;

		}
		
		/**
		 *  show create table tableName
		 */
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
			int index = stmt.indexOf("@@");
			if(index > 0 && "SELECT".equals(stmt.substring(0, index).trim().toUpperCase())){
				return analyseDoubleAtSgin(schema, rrs, stmt);
			}
			break;
		case ServerParse.DESCRIBE:// if origSQL is meta SQL, such as describe table
			int ind = stmt.indexOf(' ');
			stmt = stmt.trim();
			return analyseDescrSQL(schema, rrs, stmt, ind + 1);
		}
		return null;
	}
	
	/**
	 * 对Desc语句进行分析 返回数据路由集合
	 * 	 * 
	 * @param schema   				数据库名
	 * @param rrs    				数据路由集合
	 * @param stmt   				执行语句
	 * @param ind    				第一个' '的位置
	 * @return RouteResultset		(数据路由集合)
	 * @author mycat
	 */
	private static RouteResultset analyseDescrSQL(SchemaConfig schema,
			RouteResultset rrs, String stmt, int ind) {
		
		final String MATCHED_FEATURE = "DESCRIBE ";
		final String MATCHED2_FEATURE = "DESC ";
		int pos = 0;
		while (pos < stmt.length()) {
			char ch = stmt.charAt(pos);
			// 忽略处理注释 /* */ BEN
			if(ch == '/' &&  pos+4 < stmt.length() && stmt.charAt(pos+1) == '*') {
				if(stmt.substring(pos+2).indexOf("*/") != -1) {
					pos += stmt.substring(pos+2).indexOf("*/")+4;
					continue;
				} else {
					// 不应该发生这类情况。
					throw new IllegalArgumentException("sql 注释 语法错误");
				}
			} else if(ch == 'D'||ch == 'd') {
				// 匹配 [describe ] 
				if(pos+MATCHED_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED_FEATURE) != -1)) {
					pos = pos + MATCHED_FEATURE.length();
					break;
				} else if(pos+MATCHED2_FEATURE.length() < stmt.length() && (stmt.substring(pos).toUpperCase().indexOf(MATCHED2_FEATURE) != -1)) {
					pos = pos + MATCHED2_FEATURE.length();
					break;
				} else {
					pos++;
				}
			}
		}
		
		// 重置ind坐标。BEN GONG
		ind = pos;		
		int[] repPos = { ind, 0 };
		String tableName = RouterUtil.getTableName(stmt, repPos);
		
		stmt = stmt.substring(0, ind) + tableName + stmt.substring(repPos[1]);
		RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
		return rrs;
	}
	
	/**
	 * 根据执行语句判断数据路由
	 * 
	 * @param schema     			数据库名
	 * @param rrs		  		 	数据路由集合
	 * @param stmt		  	 		执行sql
	 * @return RouteResultset		数据路由集合
	 * @throws SQLSyntaxErrorException
	 * @author mycat
	 */
	private RouteResultset analyseDoubleAtSgin(SchemaConfig schema,
			RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {		
		String upStmt = stmt.toUpperCase();
		int atSginInd = upStmt.indexOf(" @@");
		if (atSginInd > 0) {
			return RouterUtil.routeToMultiNode(false, rrs, schema.getMetaDataNodes(), stmt);
		}
		return RouterUtil.routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}
}
