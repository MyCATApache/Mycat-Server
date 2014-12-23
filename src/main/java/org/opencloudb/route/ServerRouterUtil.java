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
package org.opencloudb.route;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.config.model.rule.RuleConfig;
import org.opencloudb.mpp.ColumnRoutePair;
import org.opencloudb.mpp.DDLParsInf;
import org.opencloudb.mpp.DDLSQLAnalyser;
import org.opencloudb.mpp.DeleteParsInf;
import org.opencloudb.mpp.DeleteSQLAnalyser;
import org.opencloudb.mpp.InsertParseInf;
import org.opencloudb.mpp.InsertSQLAnalyser;
import org.opencloudb.mpp.JoinRel;
import org.opencloudb.mpp.SelectParseInf;
import org.opencloudb.mpp.SelectSQLAnalyser;
import org.opencloudb.mpp.ShardingParseInfo;
import org.opencloudb.mpp.UpdateParsInf;
import org.opencloudb.mpp.UpdateSQLAnalyser;
import org.opencloudb.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import org.opencloudb.parser.SQLParserDelegate;
import org.opencloudb.route.function.AbstractPartitionAlgorithm;
import org.opencloudb.route.util.RouterUtil;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.StringUtil;

import com.foundationdb.sql.parser.CursorNode;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.NodeTypes;
import com.foundationdb.sql.parser.QueryTreeNode;
import com.foundationdb.sql.parser.ResultSetNode;
import com.foundationdb.sql.parser.SelectNode;

/**
 * 数据路由服务工具类
 * @deprecated  该类的逻辑转移到FdbRouteStrategy类，部分工具类方法移到RouterUtil类，该类可以废弃掉
 * @see org.opencloudb.route.impl.FdbRouteStrategy
 * @author mycat
 * 
 */

public final class ServerRouterUtil {
	private static final Logger LOGGER = Logger.getLogger(ServerRouterUtil.class);
	private static final Random rand = new Random();

	/**
	 * 根据 数据库名，执行Sql类型，执行的语句等获取数据路由集合
	 * 
	 * @param schema
	 *            数据库名
	 * @param sqlType
	 *            语句类型
	 * @param stmt
	 *            执行的语句
	 * @param charset
	 *            字符集
	 * @param info
	 * @param cachePool
	 *            the cachePool is a map of CacheStatic
	 * @return RouteResultset(数据路由集合)
	 * @throws SQLNonTransientException
	 * @author mycat
	 */
	public static RouteResultset route(SystemConfig sysConfig,
			SchemaConfig schema, int sqlType, String origSQL, String charset,
			ServerConnection sc, LayerCachePool cachePool)
			throws SQLNonTransientException {

		if (processWithMycatSeq(sysConfig,schema, sqlType, origSQL, charset,sc, cachePool) || 
			(sqlType == ServerParse.INSERT && processInsert(sysConfig,schema,sqlType,origSQL,charset,sc,cachePool))) {
			return null;
		}

		// user handler
		String stmt = MycatServer.getInstance().getSqlInterceptor().interceptSQL(origSQL, sqlType);
		
		if (origSQL != stmt && LOGGER.isDebugEnabled()) {
			LOGGER.debug("sql intercepted to " + stmt + " from " + origSQL);
		}
		if (schema.isCheckSQLSchema()) {
			stmt = removeSchema(stmt, schema.getName());
		}
		RouteResultset rrs = new RouteResultset(stmt, sqlType);
		
		// check if there is sharding in schema
		if (schema.isNoSharding()) {
			return routeToSingleNode(rrs, schema.getDataNode(), stmt);
		}
		
		RouteResultset returnedSet=routeSystemInfo(schema, sqlType, stmt, rrs);
		if(returnedSet==null){
			return routeNormalSqlWithAST(schema, stmt, rrs, charset, cachePool);
		}
		return returnedSet;
	}
	
	private static void processSQL(ServerConnection sc,SchemaConfig schema,String sql,int sqlType){
		MycatServer.getInstance().getSequnceProcessor().addNewSql(new SessionSQLPair(sc.getSession2(), schema, sql, sqlType));
	}
	private static boolean isPKInFields(String origSQL,String primaryKey,int firstLeftBracketIndex,int firstRightBracketIndex){
		boolean isPrimaryKeyInFields=false;
		String upperSQL=origSQL.substring(firstLeftBracketIndex,firstRightBracketIndex+1).toUpperCase();
		for(int pkOffset=0,primaryKeyLength=primaryKey.length(),pkStart=0;;){
			pkStart=upperSQL.indexOf(primaryKey, pkOffset);
			if(pkStart>=0 && pkStart<firstRightBracketIndex){
				char pkSide=upperSQL.charAt(pkStart-1);
				if(pkSide<=' ' || pkSide=='`' || pkSide==',' || pkSide=='('){
					pkSide=upperSQL.charAt(pkStart+primaryKey.length());
					isPrimaryKeyInFields=pkSide<=' ' || pkSide=='`' || pkSide==',' || pkSide==')';
				}
				if(isPrimaryKeyInFields){
					break;
				}
				pkOffset=pkStart+primaryKeyLength;
			}else{
				break;
			}
		}
		return isPrimaryKeyInFields;
	}
	private static void processInsert(ServerConnection sc,SchemaConfig schema,int sqlType,String origSQL,String tableName,String primaryKey,int firstLeftBracketIndex,int lastLeftBracketIndex){
		int primaryKeyLength=primaryKey.length();
		int insertSegOffset=firstLeftBracketIndex;
		String mycatSeqPrefix="next value for MYCATSEQ_";
		int mycatSeqPrefixLength=mycatSeqPrefix.length();
		int tableNameLength=tableName.length();
		
		char[] newSQLBuf=new char[origSQL.length()+primaryKeyLength+mycatSeqPrefixLength+tableNameLength+2];
		origSQL.getChars(0, firstLeftBracketIndex, newSQLBuf, 0);
		primaryKey.getChars(0,primaryKeyLength,newSQLBuf,insertSegOffset);
		insertSegOffset+=primaryKeyLength;
		newSQLBuf[insertSegOffset]=',';
		insertSegOffset++;
		origSQL.getChars(firstLeftBracketIndex,lastLeftBracketIndex,newSQLBuf,insertSegOffset);
		insertSegOffset+=lastLeftBracketIndex-firstLeftBracketIndex;
		mycatSeqPrefix.getChars(0, mycatSeqPrefixLength, newSQLBuf, insertSegOffset);
		insertSegOffset+=mycatSeqPrefixLength;
		tableName.getChars(0,tableNameLength,newSQLBuf,insertSegOffset);
		insertSegOffset+=tableNameLength;
		newSQLBuf[insertSegOffset]=',';
		insertSegOffset++;
		origSQL.getChars(lastLeftBracketIndex, origSQL.length(), newSQLBuf, insertSegOffset);
		
		processSQL(sc,schema,new String(newSQLBuf),sqlType);
	}
	private static boolean processInsert(ServerConnection sc,SchemaConfig schema,int sqlType,String origSQL,String tableName,String primaryKey){
		int firstLeftBracketIndex = origSQL.indexOf("(") + 1;
		int firstRightBracketIndex = origSQL.indexOf(")");

		boolean processedInsert=!isPKInFields(origSQL,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);
		if(processedInsert){
			processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);
		}
		return processedInsert;
	}
	private static boolean processInsert(SystemConfig sysConfig,
			SchemaConfig schema, int sqlType, String origSQL, String charset,
			ServerConnection sc, LayerCachePool cachePool){
		String tableName = StringUtil.getTableName(origSQL).toUpperCase();
		TableConfig tableConfig = schema.getTables().get(tableName);
		boolean processedInsert=false;
		if (null != tableConfig && tableConfig.isAutoIncrement()) {
			String primaryKey = tableConfig.getPrimaryKey();
			processedInsert=processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey);
		}
		return processedInsert;
	}
	private static RouteResultset routeSelect(SchemaConfig schema,QueryTreeNode ast,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		ResultSetNode rsNode = ((CursorNode) ast).getResultSetNode();
		if (rsNode instanceof SelectNode) {
			if (((SelectNode) rsNode).getFromList().isEmpty()) {
				// if it is a sql about system info, such as select charset etc.
				return routeToSingleNode(rrs, schema.getRandomDataNode(),stmt);
			}
		}
		// standard SELECT operation
		SelectParseInf parsInf = new SelectParseInf();
		parsInf.ctx = new ShardingParseInfo();
		SelectSQLAnalyser.analyse(parsInf, ast);
		return tryRouteForTables(ast, true, rrs, schema, parsInf.ctx, stmt,cachePool);
	}
	private static RouteResultset routeChildTableInsert(SchemaConfig schema,TableConfig tc,QueryTreeNode ast,InsertParseInf parsInf,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		if (tc.isChildTable()) {
			String joinKeyVal = parsInf.columnPairMap.get(tc.getJoinKey());
			if (joinKeyVal == null) {
				String inf = "joinKey not provided :" + tc.getJoinKey()+ "," + stmt;
				LOGGER.warn(inf);
				throw new SQLNonTransientException(inf);
			}
			// try to route by ER parent partion key
			RouteResultset theRrs = routeByERParentKey(stmt, rrs, tc,joinKeyVal);
			if (theRrs != null) {
				return theRrs;
			}

			// route by sql query root parent's datanode
			String findRootTBSql = tc.getLocateRTableKeySql()
					.toLowerCase() + joinKeyVal;
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("find root parent's node sql "+ findRootTBSql);
			}
			FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler();
			String dn = fetchHandler.execute(schema.getName(),findRootTBSql, tc.getRootParent().getDataNodes());
			if (dn == null) {
				throw new SQLNonTransientException("can't find (root) parent sharding node for sql:"+ stmt);
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("found partion node for child table to insert "+ dn + " sql :" + stmt);
			}
			return routeToSingleNode(rrs, dn, stmt);
		}
		return null;
	}
	private static RouteResultset routeWithPartitionColumn(SchemaConfig schema,TableConfig tc,QueryTreeNode ast,InsertParseInf parsInf,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		String partColumn = tc.getPartitionColumn();
		if (partColumn != null) {
			String sharindVal = parsInf.columnPairMap.get(partColumn);
			if (sharindVal != null) {
				Set<ColumnRoutePair> col2Val = new HashSet<ColumnRoutePair>(1);
				col2Val.add(new ColumnRoutePair(sharindVal));
				return tryRouteForTable(ast, schema, rrs, false, stmt, tc, col2Val,null, cachePool);
			} else {// must provide sharding_id when insert
				String inf = "bad insert sql (sharding column:"+ partColumn + " not provided," + stmt;
				LOGGER.warn(inf);
				throw new SQLNonTransientException(inf);
			}
		}
		return null;
	}
	private static RouteResultset routeNonGlobalInsert(SchemaConfig schema,TableConfig tc,QueryTreeNode ast,InsertParseInf parsInf,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		RouteResultset returnedSet=routeChildTableInsert(schema,tc,ast,parsInf,rrs,stmt,cachePool);
		if(returnedSet!=null){
			return returnedSet;
		}
		return routeWithPartitionColumn(schema,tc,ast,parsInf,rrs,stmt,cachePool);
	}
	private static RouteResultset routeInsert(SchemaConfig schema,QueryTreeNode ast,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		InsertParseInf parsInf = InsertSQLAnalyser.analyse(ast);
		if (parsInf.columnPairMap.isEmpty()) {
			String inf = "not supported inserq sql (columns not provided)," + stmt;
			LOGGER.warn(inf);
			throw new SQLNonTransientException(inf);
		} else if (parsInf.fromQryNode != null) {
			String inf = "not supported inserq sql (multi insert)," + stmt;
			LOGGER.warn(inf);
			throw new SQLNonTransientException(inf);
		}
		TableConfig tc = getTableConfig(schema, parsInf.tableName);
		// if is global table，set rss global table flag
		if (tc.getTableType() == TableConfig.TYPE_GLOBAL_TABLE) {
			rrs.setGlobalTable(true);
		}else {// for partition table ,partion column must provided
			RouteResultset returned=routeNonGlobalInsert(schema,tc,ast,parsInf,rrs,stmt, cachePool);
			if(returned!=null){
				return returned;
			}
		}
		return tryRouteForTable(ast, schema, rrs, false, stmt, tc, null,null, cachePool);
	}
	private static RouteResultset routeUpdate(SchemaConfig schema,QueryTreeNode ast,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		UpdateParsInf parsInf = UpdateSQLAnalyser.analyse(ast);
		// check if sharding columns is updated
		TableConfig tc = getTableConfig(schema, parsInf.tableName);
		// if is global table，set rss global table flag
		if (tc.getTableType() == TableConfig.TYPE_GLOBAL_TABLE) {
			rrs.setGlobalTable(true);
		}
		if (parsInf.columnPairMap.containsKey(tc.getPartitionColumn())) {
			throw new SQLNonTransientException("partion key can't be updated " + parsInf.tableName+ "->" + tc.getPartitionColumn());
		} else if (parsInf.columnPairMap.containsKey(tc.getJoinKey())) {
			// ,child and parent tables relation column can't be updated
			throw new SQLNonTransientException("parent relation column can't be updated "+ parsInf.tableName + "->" + tc.getJoinKey());
		}
		if (parsInf.ctx == null) {// no where condtion
			return tryRouteForTable(ast, schema, rrs, false, stmt, tc,null, null, cachePool);
		} else if (tc.getTableType() == TableConfig.TYPE_GLOBAL_TABLE && 
				    parsInf.ctx.tablesAndConditions.size() > 1) {
			throw new SQLNonTransientException("global table not supported multi table related update "+ parsInf.tableName);
		}
		return tryRouteForTables(ast, false, rrs, schema, parsInf.ctx,stmt, cachePool);
	}
	private static RouteResultset routeDelete(SchemaConfig schema,QueryTreeNode ast,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		DeleteParsInf parsInf = DeleteSQLAnalyser.analyse(ast);
		// if is global table，set rss global table flag
		TableConfig tc = getTableConfig(schema, parsInf.tableName);
		if (tc.getTableType() == TableConfig.TYPE_GLOBAL_TABLE) {
			rrs.setGlobalTable(true);
		}
		if (parsInf.ctx != null) {
			return tryRouteForTables(ast, false, rrs, schema, parsInf.ctx,stmt, cachePool);
		} else {// no where condtion
			return tryRouteForTable(ast, schema, rrs, false, stmt, tc,null, null, cachePool);
		}
	}
	private static RouteResultset routeDDL(SchemaConfig schema,QueryTreeNode ast,RouteResultset rrs,String stmt, LayerCachePool cachePool) throws SQLNonTransientException{
		DDLParsInf parsInf = DDLSQLAnalyser.analyse(ast);
		TableConfig tc = getTableConfig(schema, parsInf.tableName);

		// if is global table，set rss global table flag
		if (tc.getTableType() == TableConfig.TYPE_GLOBAL_TABLE) {
			rrs.setGlobalTable(true);
		}

		return routeToMultiNode(schema, false, false, ast, rrs,tc.getDataNodes(), stmt);
	}
	private static boolean processWithMycatSeq(SystemConfig sysConfig,
			SchemaConfig schema, int sqlType, String origSQL, String charset,
			ServerConnection sc, LayerCachePool cachePool){
		// check if origSQL is with global sequence
		// @micmiu it is just a simple judgement
		if (origSQL.indexOf(" MYCATSEQ_") != -1) {
			processSQL(sc,schema,origSQL,sqlType);
			return true;
		}
		return false;
	}
	private static RouteResultset routeSystemInfo(SchemaConfig schema,int sqlType,String stmt,RouteResultset rrs) throws SQLSyntaxErrorException{
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
	private static RouteResultset routeNormalSqlWithAST(SchemaConfig schema,String stmt,RouteResultset rrs,String charset,LayerCachePool cachePool) throws SQLNonTransientException{
		// to generate and expand AST
		QueryTreeNode ast = SQLParserDelegate.parse(stmt,charset == null ? "utf-8" : charset);
		switch(ast.getNodeType()){
		case NodeTypes.CURSOR_NODE://select
			return routeSelect(schema,ast,rrs,stmt, cachePool);
		case NodeTypes.INSERT_NODE:
			return routeInsert(schema,ast,rrs,stmt, cachePool);
		case NodeTypes.UPDATE_NODE:
			return routeUpdate(schema,ast,rrs,stmt, cachePool);
		case NodeTypes.DELETE_NODE:
			return routeDelete(schema,ast,rrs,stmt, cachePool);
		}
		if (ast instanceof DDLStatementNode) {
			return routeDDL(schema,ast,rrs,stmt, cachePool);
		} else {
			LOGGER.info("TODO ,support sql type "+ ast.getClass().getCanonicalName() + " ," + stmt);
			return rrs;
		}
	}
	

	/**
	 * 根据 ER分片规则获取路由集合
	 * 
	 * @param stmt
	 *            执行的语句
	 * @param rrs
	 *            数据路由集合
	 * @param tc
	 *            表实体
	 * @param joinKeyVal
	 *            连接属性
	 * @return RouteResultset(数据路由集合)
	 * @throws SQLNonTransientException
	 * @author mycat
	 */

	private static RouteResultset routeByERParentKey(String stmt,
			RouteResultset rrs, TableConfig tc, String joinKeyVal)
			throws SQLNonTransientException {
		// only has one parent level and ER parent key is parent
		// table's partition key
		if (tc.isSecondLevel()
				&& tc.getParentTC().getPartitionColumn()
						.equals(tc.getParentKey())) { // using
														// parent
														// rule to
														// find
														// datanode
			Set<ColumnRoutePair> parentColVal = new HashSet<ColumnRoutePair>(1);
			ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
			parentColVal.add(pair);
			Set<String> dataNodeSet = ruleCalculate(tc.getParentTC(),
					parentColVal);
			if (dataNodeSet.isEmpty() || dataNodeSet.size() > 1) {
				throw new SQLNonTransientException(
						"parent key can't find  valid datanode ,expect 1 but found: "
								+ dataNodeSet.size());
			}
			String dn = dataNodeSet.iterator().next();
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  "
						+ dn + " sql :" + stmt);
			}
			return routeToSingleNode(rrs, dn, stmt);
		}
		return null;
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 * 
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author mycat
	 */
	private static int[] getSpecPos(String upStmt, int start) {
		String token1 = " FROM ";
		String token2 = " IN ";
		int tabInd1 = upStmt.indexOf(token1, start);
		int tabInd2 = upStmt.indexOf(token2, start);
		if (tabInd1 > 0) {
			if (tabInd2 < 0) {
				return new int[] { tabInd1, token1.length() };
			}
			return (tabInd1 < tabInd2) ? new int[] { tabInd1, token1.length() }
					: new int[] { tabInd2, token2.length() };
		} else {
			return new int[] { tabInd2, token2.length() };
		}
	}

	/**
	 * 根据表明获取表的对象
	 * 
	 * @param schema
	 *            数据库名
	 * @param tableName
	 *            表名
	 * @return TableConfig(表的对象)
	 * @throws SQLNonTransientException
	 * @author mycat
	 */
	private static TableConfig getTableConfig(SchemaConfig schema,
			String tableName) throws SQLNonTransientException {
		TableConfig tc = schema.getTables().get(tableName);
		if (tc == null) {
			String msg = "can't find table define in schema ,table:"
					+ tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		return tc;
	}

	/**
	 * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
	 * 
	 * @param upStmt
	 *            执行sql
	 * @param start
	 *            开发位置
	 * @return int
	 * @author mycat
	 */
	private static int getSpecEndPos(String upStmt, int start) {
		int tabInd = upStmt.indexOf(" LIKE ", start);
		if (tabInd < 0) {
			tabInd = upStmt.indexOf(" WHERE ", start);
		}
		if (tabInd < 0) {
			return upStmt.length();
		}
		return tabInd;
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
	private static RouteResultset analyseDoubleAtSgin(SchemaConfig schema,
			RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
		String upStmt = stmt.toUpperCase();

		int atSginInd = upStmt.indexOf(" @@");
		if (atSginInd > 0) {
			return routeToMultiNode(schema, false, false, null, rrs,
					schema.getMetaDataNodes(), stmt);
		}

		return routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}

	/**
	 * 根据show语句获取数据路由集合
	 * 
	 * @param schema
	 *            数据库名
	 * @param rrs
	 *            数据路由集合
	 * @param stmt
	 *            执行的语句
	 * @return RouteResultset数据路由集合
	 * @throws SQLSyntaxErrorException
	 * @author mycat
	 */
	private static RouteResultset analyseShowSQL(SchemaConfig schema,
			RouteResultset rrs, String stmt) throws SQLSyntaxErrorException {
		String upStmt = stmt.toUpperCase();
		int tabInd = upStmt.indexOf(" TABLES");
		if (tabInd > 0) {// show tables
			int[] nextPost = getSpecPos(upStmt, 0);
			if (nextPost[0] > 0) {// remove db info
				int end = getSpecEndPos(upStmt, tabInd);
				if (upStmt.indexOf(" FULL") > 0) {
					stmt = "SHOW FULL TABLES" + stmt.substring(end);
				} else {
					stmt = "SHOW TABLES" + stmt.substring(end);
				}
			}
			return routeToMultiNode(schema, false, false, null, rrs,
					schema.getMetaDataNodes(), stmt);
		}
		// show index or column
		int[] indx = getSpecPos(upStmt, 0);
		if (indx[0] > 0) {
			// has table
			int[] repPos = { indx[0] + indx[1], 0 };
			String tableName = getTableName(stmt, repPos);
			// IN DB pattern
			int[] indx2 = getSpecPos(upStmt, indx[0] + indx[1] + 1);
			if (indx2[0] > 0) {// find LIKE OR WHERE
				repPos[1] = getSpecEndPos(upStmt, indx2[0] + indx2[1]);

			}
			stmt = stmt.substring(0, indx[0]) + " FROM " + tableName
					+ stmt.substring(repPos[1]);
			RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
			return rrs;

		}
		// show create table tableName
		int[] createTabInd = getCreateTablePos(upStmt, 0);
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

		return routeToSingleNode(rrs, schema.getRandomDataNode(), stmt);
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 * 
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author mycat
	 */
	private static int[] getCreateTablePos(String upStmt, int start) {
		String token1 = " CREATE ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd > 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取table名字
	 * 
	 * @param stmt
	 *            执行语句
	 * @param repPos
	 *            开始位置和位数
	 * @return 表名
	 * @author mycat
	 */
	private static String getTableName(String stmt, int[] repPos) {
		int startPos = repPos[0];
		int secInd = stmt.indexOf(' ', startPos + 1);
		if (secInd < 0) {
			secInd = stmt.length();
		}
		repPos[1] = secInd;
		String tableName = stmt.substring(startPos, secInd).trim();
		int ind2 = tableName.indexOf('.');
		if (ind2 > 0) {
			tableName = tableName.substring(ind2 + 1);
		}
		return tableName;
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
		String tableName = getTableName(stmt, repPos);
		stmt = stmt.substring(0, ind) + tableName + stmt.substring(repPos[1]);
		RouterUtil.routeForTableMeta(rrs, schema, tableName, stmt);
		return rrs;
	}

	private static String addSQLLmit(SchemaConfig schema, RouteResultset rrs,
			QueryTreeNode ast, String sql) throws SQLSyntaxErrorException {
		if (!rrs.hasPrimaryKeyToCache() && schema.getDefaultMaxLimit() != -1
				&& ast instanceof CursorNode
				&& ((CursorNode) ast).getFetchFirstClause() == null) {
			String newstmt = SelectSQLAnalyser.addLimitCondtionForSelectSQL(
					rrs, (CursorNode) ast, schema.getDefaultMaxLimit());
			if (newstmt != null) {
				return newstmt;
			}
		}
		return sql;
	}

	/**
	 * 简单描述该方法的实现功能
	 * 
	 * @param ast
	 *            QueryTreeNode
	 * @param schema
	 *            数据库名
	 * @param rrs
	 *            数据路由集合
	 * @param isSelect
	 *            是否是select语句标志
	 * @param sql
	 *            执行语句
	 * @param tc
	 *            表实体
	 * @param ruleCol2Val
	 *            一个ColumnRoutePair集合
	 * @param allColConds
	 *            一个ColumnRoutePair集合
	 * @param cachePool
	 * @return 一个数据路由集合
	 * @throws SQLNonTransientException
	 * @author mycat
	 */
	private static RouteResultset tryRouteForTable(QueryTreeNode ast,
			SchemaConfig schema, RouteResultset rrs, boolean isSelect,
			String sql, TableConfig tc, Set<ColumnRoutePair> ruleCol2Val,
			Map<String, Set<ColumnRoutePair>> allColConds,
			LayerCachePool cachePool) throws SQLNonTransientException {

		if (tc.getTableType() == TableConfig.TYPE_GLOBAL_TABLE && isSelect) {
			sql = addSQLLmit(schema, rrs, ast, sql);
			return routeToSingleNode(rrs, tc.getRandomDataNode(), sql);
		}

		// no partion define or no where condtion for this table or no
		// partion column condtions
		boolean cache = isSelect;
		if (ruleCol2Val == null || ruleCol2Val.isEmpty()) {
			if (tc.isRuleRequired()) {
				throw new IllegalArgumentException("route rule for table "
						+ tc.getName() + " is required: " + sql);

			} else if (allColConds != null && allColConds.size() == 1) {
				// try if can route by ER relation
				if (tc.isSecondLevel()
						&& tc.getParentTC().getPartitionColumn()
								.equals(tc.getParentKey())) {
					Set<ColumnRoutePair> joinKeyPairs = allColConds.get(tc
							.getJoinKey());
					if (joinKeyPairs != null) {
						Set<String> dataNodeSet = ruleCalculate(
								tc.getParentTC(), joinKeyPairs);
						if (dataNodeSet.isEmpty()) {
							throw new SQLNonTransientException(
									"parent key can't find any valid datanode ");
						}
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  "
									+ Arrays.toString(dataNodeSet.toArray())
									+ " sql :" + sql);
						}
						if (dataNodeSet.size() > 1) {
							return routeToMultiNode(schema, isSelect, isSelect,
									ast, rrs, dataNodeSet, sql);
						} else {
							rrs.setCacheAble(true);
							return routeToSingleNode(rrs, dataNodeSet
									.iterator().next(), sql);
						}
					}

				}

				// try by primary key if found in cache
				Set<ColumnRoutePair> primaryKeyPairs = allColConds.get(tc
						.getPrimaryKey());
				if (primaryKeyPairs != null) {
					if (LOGGER.isInfoEnabled()) {
						LOGGER.info("try to find cache by primary key ");
					}
					cache = false;
					Set<String> dataNodes = new HashSet<String>(
							primaryKeyPairs.size());
					boolean allFound = true;
					String tableKey = schema.getName() + '_' + tc.getName();
					for (ColumnRoutePair pair : primaryKeyPairs) {
						String cacheKey = pair.colValue;
						String dataNode = (String) cachePool.get(tableKey,
								cacheKey);
						if (dataNode == null) {
							allFound = false;
							break;
						} else {
							dataNodes.add(dataNode);
						}
					}
					if (allFound) {
						return routeToMultiNode(schema, isSelect, isSelect,
								ast, rrs, dataNodes, sql);
					}
					// need cache primary key ->datanode relation
					if (isSelect && tc.getPrimaryKey() != null) {
						rrs.setPrimaryKey(tableKey + '.' + tc.getPrimaryKey());
					}
				}

			}
			return routeToMultiNode(schema, isSelect, cache, ast, rrs,
					tc.getDataNodes(), sql);
		}
		// match table with where condtion of partion colum values
		Set<String> dataNodeSet = ruleCalculate(tc, ruleCol2Val);
		if (dataNodeSet.size() == 1) {
			rrs.setCacheAble(isSelect);
			return routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
		} else {
			return routeToMultiNode(schema, isSelect, isSelect, ast, rrs,
					dataNodeSet, sql);
		}

	}

	/**
	 * 简单描述该方法的实现功能
	 * 
	 * @param ast
	 *            QueryTreeNode
	 * @param isSelect
	 *            是否是select语句
	 * @param rrs
	 *            数据路由集合
	 * @param schema
	 *            数据库名 the name of datebase
	 * @param ctx
	 *            ShardingParseInfo(分片)
	 * @param sql
	 *            执行sql
	 * @param cachePool
	 * @return 一个数据路由集合
	 * @throws SQLNonTransientException
	 * @author mycat
	 */
	private static RouteResultset tryRouteForTables(QueryTreeNode ast,
			boolean isSelect, RouteResultset rrs, SchemaConfig schema,
			ShardingParseInfo ctx, String sql, LayerCachePool cachePool)
			throws SQLNonTransientException {
		Map<String, TableConfig> tables = schema.getTables();
		Map<String, Map<String, Set<ColumnRoutePair>>> tbCondMap = ctx.tablesAndConditions;
		if (tbCondMap.size() == 1) {
			// only one table in this sql
			Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry = tbCondMap
					.entrySet().iterator().next();
			TableConfig tc = getTableConfig(schema, entry.getKey());
			if (tc.getRule() == null && tc.getDataNodes().size() == 1) {
				rrs.setCacheAble(isSelect);
				// 20140625 修复 配置为全局表单节点的语句不会自动加上limit
				sql = addSQLLmit(schema, rrs, ast, sql);
				return routeToSingleNode(rrs, tc.getDataNodes().get(0), sql);
			}
			Map<String, Set<ColumnRoutePair>> colConds = entry.getValue();

			return tryRouteForTable(ast, schema, rrs, isSelect, sql, tc,
					colConds.get(tc.getPartitionColumn()), colConds, cachePool);
		} else if (!ctx.joinList.isEmpty()) {
			for (JoinRel joinRel : ctx.joinList) {
				TableConfig rootc = schema.getJoinRel2TableMap().get(
						joinRel.joinSQLExp);
				if (rootc == null) {
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("can't find join relation in schema "
								+ schema.getName() + " :" + joinRel.joinSQLExp
								+ " maybe global table join");
					}

				} else {
					if (rootc.getName().equals(joinRel.tableA)) {
						// table a is root table
						tbCondMap.remove(joinRel.tableB);
					} else if (rootc.getName().equals(joinRel.tableB)) {
						// table B is root table
						tbCondMap.remove(joinRel.tableA);
					} else if (tbCondMap.containsKey(rootc.getName())) {
						// contains root table in sql ,then remove all child
						tbCondMap.remove(joinRel.tableA);
						tbCondMap.remove(joinRel.tableB);
					} else {// both there A and B are not root table，remove any
							// one
						tbCondMap.remove(joinRel.tableA);
					}

				}
			}
		}

		if (tbCondMap.size() > 1) {

			Set<String> curRNodeSet = new LinkedHashSet<String>();
			Set<String> routePairSet = new LinkedHashSet<String>();// 拆分字段后路由节点

			String curTableName = null;
			Map<String, ArrayList<String>> globalTableDataNodesMap = new LinkedHashMap<String, ArrayList<String>>();
			for (Entry<String, Map<String, Set<ColumnRoutePair>>> e : tbCondMap
					.entrySet()) {
				String tableName = e.getKey();
				Map<String, Set<ColumnRoutePair>> col2ValMap = e.getValue();
				TableConfig tc = tables.get(tableName);
				if (tc == null) {
					String msg = "can't find table define in schema "
							+ tableName + " schema:" + schema.getName();
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				} else if (tc.getTableType() == TableConfig.TYPE_GLOBAL_TABLE) {
					// add to globalTablelist
					globalTableDataNodesMap
							.put(tc.getName(), tc.getDataNodes());
					continue;
				}
				Collection<String> newDataNodes = null;
				String partColmn = tc.getPartitionColumn();
				Set<ColumnRoutePair> col2Val = partColmn == null ? null
						: col2ValMap.get(partColmn);
				if (col2Val == null || col2Val.isEmpty()) {
					if (tc.isRuleRequired()) {
						throw new IllegalArgumentException(
								"route rule for table " + tableName
										+ " is required: " + sql);
					}
					newDataNodes = tc.getDataNodes();

				} else {
					// match table with where condtion of partion colum values
					newDataNodes = ruleCalculate(tc, col2Val);

				}

				if (curRNodeSet.isEmpty()) {
					curTableName = tc.getName();
					curRNodeSet.addAll(newDataNodes);
					if (col2Val != null && !col2Val.isEmpty()) {
						routePairSet.addAll(newDataNodes);
					}
				} else {
					if (col2Val == null || col2Val.isEmpty()) {

						if (curRNodeSet.retainAll(newDataNodes)
								&& routePairSet.isEmpty()) {
							String errMsg = "invalid route in sql, multi global tables found but datanode has no intersection "
									+ " sql:" + sql;
							LOGGER.warn(errMsg);
							throw new SQLNonTransientException(errMsg);

						}

					} else {

						if (routePairSet.isEmpty()) {
							routePairSet.addAll(newDataNodes);
						} else if (!checkIfValidMultiTableRoute(routePairSet,
								newDataNodes)
								|| (curRNodeSet.retainAll(newDataNodes) && routePairSet
										.isEmpty())) {
							String errMsg = "invalid route in sql, "
									+ routePairSet + " route to :"
									+ Arrays.toString(routePairSet.toArray())
									+ " ,but " + tc.getName() + " to "
									+ Arrays.toString(newDataNodes.toArray())
									+ " sql:" + sql;
							LOGGER.warn(errMsg);
							throw new SQLNonTransientException(errMsg);

						}

					}

					// if (!checkIfValidMultiTableRoute(curRNodeSet,
					// newDataNodes)) {
					// String errMsg = "invalid route in sql, " + curTableName
					// + " route to :"
					// + Arrays.toString(curRNodeSet.toArray())
					// + " ,but " + tc.getName() + " to "
					// + Arrays.toString(newDataNodes.toArray())
					// + " sql:" + sql;
					// LOGGER.warn(errMsg);
					// throw new SQLNonTransientException(errMsg);
					// }
				}

			}
			// only global table contains in sql
			if (!globalTableDataNodesMap.isEmpty() && curRNodeSet.isEmpty()) {
				ArrayList<String> resultList = new ArrayList<String>();
				for (ArrayList<String> nodeList : globalTableDataNodesMap
						.values()) {
					if (resultList.isEmpty()) {
						resultList = nodeList;

					} else {
						if (resultList.retainAll(nodeList)
								&& resultList.isEmpty()) {
							String errMsg = "invalid route in sql, multi global tables found but datanode has no intersection "
									+ " sql:" + sql;
							LOGGER.warn(errMsg);
							throw new SQLNonTransientException(errMsg);
						}

					}

				}
				if (resultList.size() == 1) {
					rrs.setCacheAble(true);
					sql = addSQLLmit(schema, rrs, ast, sql);
					rrs = routeToSingleNode(rrs, resultList.get(0), sql);
				} else {
					// mulit routes ,not cache route result
					rrs.setCacheAble(false);
					rrs = routeToSingleNode(rrs, getRandomDataNode(resultList),
							sql);
				}
				return rrs;
			} else if (!globalTableDataNodesMap.isEmpty()
					&& !curRNodeSet.isEmpty()) {
				// judge if global table contains all dataNodes of other tables
				for (Map.Entry<String, ArrayList<String>> entry : globalTableDataNodesMap
						.entrySet()) {
					if (!entry.getValue().containsAll(curRNodeSet)) {
						String errMsg = "invalid route in sql, " + curTableName
								+ " route to :"
								+ Arrays.toString(curRNodeSet.toArray())
								+ " ,but " + entry.getKey() + " to "
								+ Arrays.toString(entry.getValue().toArray())
								+ " sql:" + sql;
						LOGGER.warn(errMsg);
						throw new SQLNonTransientException(errMsg);
					}
				}

			}

			if (curRNodeSet.size() > 1) {
				LOGGER.warn("multi route tables found in this sql ,tables:"
						+ Arrays.toString(tbCondMap.keySet().toArray())
						+ " sql:" + sql);
				return routeToMultiNode(schema, isSelect, isSelect, ast, rrs,
						curRNodeSet, sql);
			} else {
				return routeToSingleNode(rrs, curRNodeSet.iterator().next(),
						sql);

			}
		} else {// only one table
			Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry = tbCondMap
					.entrySet().iterator().next();
			Map<String, Set<ColumnRoutePair>> allColValues = entry.getValue();
			TableConfig tc = getTableConfig(schema, entry.getKey());
			return tryRouteForTable(ast, schema, rrs, isSelect, sql, tc,
					allColValues.get(tc.getPartitionColumn()), allColValues,
					cachePool);
		}
	}

	/**
	 * 判断两个表所在节点集合是否相等
	 * 
	 * @param curRNodeSet
	 *            当前表所在的节点集合
	 * @param newNodeSet
	 *            新表所在节点集合
	 * @return 返回fase(不相等)或true(相等)
	 * @author mycat
	 */
	private static boolean checkIfValidMultiTableRoute(Set<String> curRNodeSet,
			Collection<String> newNodeSet) {
		if (curRNodeSet.size() != newNodeSet.size()) {
			return false;
		} else {
			for (String dataNode : newNodeSet) {
				if (!curRNodeSet.contains(dataNode)) {
					return false;
				}
			}
		}
		return true;

	}

	/**
	 * 获取第一个节点作为路由
	 * 
	 * @param rrs
	 *            数据路由集合
	 * @param dataNode
	 *            数据库所在节点
	 * @param stmt
	 *            执行语句
	 * @return 数据路由集合
	 * @author mycat
	 */
	private static RouteResultset routeToSingleNode(RouteResultset rrs,
			String dataNode, String stmt) {
		if (dataNode == null) {
			return rrs;
		}
		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);
		rrs.setNodes(nodes);
		return rrs;
	}

	private static RouteResultset routeToMultiNode(SchemaConfig schema,
			boolean isSelect, boolean cache, QueryTreeNode ast,
			RouteResultset rrs, Collection<String> dataNodes, String stmt)
			throws SQLSyntaxErrorException {
		if (isSelect) {
			String sql = SelectSQLAnalyser.analyseMergeInf(rrs, ast, true,
					schema.getDefaultMaxLimit());
			if (sql != null) {
				stmt = sql;
			}
		}
		RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
		int i = 0;
		for (String dataNode : dataNodes) {

			nodes[i++] = new RouteResultsetNode(dataNode, rrs.getSqlType(),
					stmt);
		}
		rrs.setCacheAble(cache);
		rrs.setNodes(nodes);
		return rrs;
	}

	private static String getRandomDataNode(ArrayList<String> dataNodes) {
		int index = Math.abs(rand.nextInt()) % dataNodes.size();
		return dataNodes.get(index);
	}

	/**
	 * 移除执行语句中的数据库名
	 * 
	 * @param stmt
	 *            执行语句
	 * @param schema
	 *            数据库名
	 * @return 执行语句
	 * @author mycat
	 */

	private static String removeSchema(String stmt, String schema) {
		final String upStmt = stmt.toUpperCase();
		final String upSchema = schema.toUpperCase() + ".";
		int strtPos = 0;
		int indx = 0;
		boolean flag = false;
		indx = upStmt.indexOf(upSchema, strtPos);
		if (indx < 0) {
			StringBuilder sb = new StringBuilder("`").append(
					schema.toUpperCase()).append("`.");
			indx = upStmt.indexOf(sb.toString(), strtPos);
			flag = true;
			if (indx < 0) {
				return stmt;
			}
		}
		StringBuilder sb = new StringBuilder();
		while (indx > 0) {
			sb.append(stmt.substring(strtPos, indx));
			strtPos = indx + upSchema.length();
			if (flag) {
				strtPos += 2;
			}
			indx = upStmt.indexOf(upSchema, strtPos);
		}
		sb.append(stmt.substring(strtPos));
		return sb.toString();
	}

	/**
	 * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
	 */
	private static Set<String> ruleCalculate(TableConfig tc,
			Set<ColumnRoutePair> colRoutePairSet) {
		Set<String> routeNodeSet = new LinkedHashSet<String>();
		String col = tc.getRule().getColumn();
		RuleConfig rule = tc.getRule();
		AbstractPartitionAlgorithm algorithm = rule.getRuleAlgorithm();
		for (ColumnRoutePair colPair : colRoutePairSet) {
			if (colPair.colValue != null) {
				Integer nodeIndx = algorithm.calculate(colPair.colValue);
				if (nodeIndx == null) {
					throw new IllegalArgumentException(
							"can't find datanode for sharding column:" + col
									+ " val:" + colPair.colValue);
				} else {
					String dataNode = tc.getDataNodes().get(nodeIndx);
					routeNodeSet.add(dataNode);
					colPair.setNodeId(nodeIndx);
				}
			} else if (colPair.rangeValue != null) {
				Integer[] nodeRange = algorithm.calculateRange(
						String.valueOf(colPair.rangeValue.beginValue),
						String.valueOf(colPair.rangeValue.endValue));
				if (nodeRange != null) {
					/**
					 * 不能确认 colPair的 nodeid是否会有其它影响
					 */
					if (nodeRange.length == 0) {
						routeNodeSet.addAll(tc.getDataNodes());
					} else {
						ArrayList<String> dataNodes = tc.getDataNodes();
						String dataNode = null;
						for (Integer nodeId : nodeRange) {
							dataNode = dataNodes.get(nodeId);
							routeNodeSet.add(dataNode);
						}
					}
				}
			}

		}
		return routeNodeSet;
	}

	public static void main(String[] args) {
		String sql="insert into customer (id,name,company_id,sharding_id)values(1,'wang',1,10000);";
		boolean b=isPKInFields(sql, "ID", sql.indexOf('('), sql.indexOf(')'));
		System.out.println(b);
		
		String origSQL="insert into user(name,code,password)values('name','code','password')";
		int firstLeftBracketIndex = origSQL.indexOf("(") + 1;
		int firstRightBracketIndex = origSQL.indexOf(")");
		int lastLeftBracketIndex = origSQL.lastIndexOf("(") + 1;
		String tableName = StringUtil.getTableName(origSQL).toUpperCase();
		String primaryKey="ID";
		String newSQL=null;
		long s=0;
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			int primaryKeyLength=primaryKey.length();
			int insertSegOffset=firstLeftBracketIndex;
			String mycatSeqPrefix="next value for MYCATSEQ_";
			int mycatSeqPrefixLength=mycatSeqPrefix.length();
			int tableNameLength=tableName.length();
			StringBuilder newSQLBuilder=new StringBuilder(origSQL).insert(insertSegOffset,primaryKey);
			
			insertSegOffset+=primaryKeyLength;
			newSQLBuilder.insert(insertSegOffset,',');
			
			insertSegOffset=lastLeftBracketIndex+primaryKeyLength+1;
		    newSQLBuilder.insert(insertSegOffset,mycatSeqPrefix);
		    
		    insertSegOffset+=mycatSeqPrefixLength;
		    newSQLBuilder.insert(insertSegOffset, tableName).insert(insertSegOffset+tableNameLength, ',');
			newSQL=newSQLBuilder.toString();
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			int primaryKeyLength=primaryKey.length();
			String mycatSeqPrefix="next value for MYCATSEQ_";
			int mycatSeqPrefixLength=mycatSeqPrefix.length();
			int tableNameLength=tableName.length();
		
			StringBuilder newSQLBuilder=new StringBuilder(
					origSQL.length()+primaryKeyLength+mycatSeqPrefixLength+tableNameLength+4);//to prevent StringBuilder to expand capacity
	
			newSQLBuilder.append(origSQL,0,firstLeftBracketIndex)
			             .append(primaryKey).append(',')
			             .append(origSQL,firstLeftBracketIndex,lastLeftBracketIndex)
			             .append(mycatSeqPrefix).append(tableName).append(',')
			             .append(origSQL,lastLeftBracketIndex,origSQL.length());
			newSQL=newSQLBuilder.toString();
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			int primaryKeyLength=primaryKey.length();
			int insertSegOffset=firstLeftBracketIndex;
			String mycatSeqPrefix="next value for MYCATSEQ_";
			int mycatSeqPrefixLength=mycatSeqPrefix.length();
			int tableNameLength=tableName.length();
			StringBuilder newSQLBuilder=new StringBuilder((origSQL.length()+primaryKeyLength+mycatSeqPrefixLength+tableNameLength+2)*2+2)//to prevent StringBuilder to expand capacity
			                           .append(origSQL).insert(insertSegOffset,primaryKey);
			
			insertSegOffset+=primaryKeyLength;
			newSQLBuilder.insert(insertSegOffset,',');
			
			insertSegOffset=lastLeftBracketIndex+primaryKeyLength+1;
		    newSQLBuilder.insert(insertSegOffset,mycatSeqPrefix);
		    
		    insertSegOffset+=mycatSeqPrefixLength;
		    newSQLBuilder.insert(insertSegOffset, tableName).insert(insertSegOffset+tableNameLength, ',');
			newSQL=newSQLBuilder.toString();
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			int primaryKeyLength=primaryKey.length();
			int insertSegOffset=firstLeftBracketIndex;
			String mycatSeqPrefix="next value for MYCATSEQ_";
			int mycatSeqPrefixLength=mycatSeqPrefix.length();
			int tableNameLength=tableName.length();
			StringBuilder newSQLBuilder=new StringBuilder(origSQL.length()+primaryKeyLength+mycatSeqPrefixLength+tableNameLength+4)//to prevent StringBuilder to expand capacity
			                           .append(origSQL).insert(insertSegOffset,primaryKey);
			
			insertSegOffset+=primaryKeyLength;
			newSQLBuilder.insert(insertSegOffset,',');
			
			insertSegOffset=lastLeftBracketIndex+primaryKeyLength+1;
		    newSQLBuilder.insert(insertSegOffset,mycatSeqPrefix);
		    
		    insertSegOffset+=mycatSeqPrefixLength;
		    newSQLBuilder.insert(insertSegOffset, tableName).insert(insertSegOffset+tableNameLength, ',');
			newSQL=newSQLBuilder.toString();
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			newSQL=origSQL.substring(0, firstLeftBracketIndex)+
				      primaryKey+','+
				      origSQL.substring(firstLeftBracketIndex,lastLeftBracketIndex)+
				      "next value for MYCATSEQ_"+tableName+','+
				      origSQL.substring(lastLeftBracketIndex,origSQL.length());
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			StringBuilder segSQL=new StringBuilder();
			StringBuilder newSQLBuilder=new StringBuilder(origSQL).insert(firstLeftBracketIndex,segSQL.append(primaryKey).append(','));
			segSQL.delete(0, segSQL.length());
			newSQLBuilder.insert(lastLeftBracketIndex+primaryKey.length()+1, segSQL.append("next value for MYCATSEQ_").append(tableName).append(','));
			newSQL=newSQLBuilder.toString();
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			int primaryKeyLength=primaryKey.length();
			int insertSegOffset=firstLeftBracketIndex;
			String mycatSeqPrefix="next value for MYCATSEQ_";
			int mycatSeqPrefixLength=mycatSeqPrefix.length();
			int tableNameLength=tableName.length();
			char[] newSQLBuf=new char[origSQL.length()+primaryKeyLength+mycatSeqPrefixLength+tableNameLength+2];
			
			origSQL.getChars(0, firstLeftBracketIndex, newSQLBuf, 0);
			primaryKey.getChars(0,primaryKeyLength,newSQLBuf,insertSegOffset);
			insertSegOffset+=primaryKeyLength;
			newSQLBuf[insertSegOffset]=',';
			insertSegOffset++;
			origSQL.getChars(firstLeftBracketIndex,lastLeftBracketIndex,newSQLBuf,insertSegOffset);
			insertSegOffset+=lastLeftBracketIndex-firstLeftBracketIndex;
			mycatSeqPrefix.getChars(0, mycatSeqPrefixLength, newSQLBuf, insertSegOffset);
			insertSegOffset+=mycatSeqPrefixLength;
			tableName.getChars(0,tableNameLength,newSQLBuf,insertSegOffset);
			insertSegOffset+=tableNameLength;
			newSQLBuf[insertSegOffset]=',';
			insertSegOffset++;
			origSQL.getChars(lastLeftBracketIndex, origSQL.length(), newSQLBuf, insertSegOffset);
			newSQL=new String(newSQLBuf);
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			StringBuilder sb = new StringBuilder();
			sb.append(origSQL.substring(0, firstLeftBracketIndex));
			sb.append(primaryKey);
			sb.append(",");
			sb.append(origSQL.substring(firstLeftBracketIndex,
					lastLeftBracketIndex));
			sb.append("next value for MYCATSEQ_");
			sb.append(tableName);
			sb.append(",");
			sb.append(origSQL.substring(lastLeftBracketIndex,
					origSQL.length()));
			newSQL=sb.toString();
		}
		System.out.println(System.currentTimeMillis()-s);
		System.out.println(newSQL);

		s=System.currentTimeMillis();
		for(int i=0;i<10_0000;i++){
			String insertFieldsSQL = origSQL.substring(
					firstLeftBracketIndex, firstRightBracketIndex);
			String[] insertFields = insertFieldsSQL.split(",");
	
			boolean isPrimaryKeyInFields = false;
			for (String field : insertFields) {
				if (field.toUpperCase().equals(primaryKey)) {
					isPrimaryKeyInFields = true;
					break;
				}
			}
		}
		System.out.println(System.currentTimeMillis()-s);
		
		s=System.currentTimeMillis();
		for(int i=0;i<10_0000;i++){
			boolean result=false;
			int pkOffset=0;
			int primaryKeyLength=primaryKey.length();
			String upperSQL=origSQL.substring(firstLeftBracketIndex,firstRightBracketIndex).toUpperCase();
			do{
				int pkStart=upperSQL.indexOf(primaryKey, pkOffset);
				if(pkStart>=0 && pkStart<firstRightBracketIndex){
					char pkSide=origSQL.charAt(pkStart-1);
					if(pkSide<=' ' || pkSide=='`' || pkSide==',' || pkSide=='('){
						pkSide=origSQL.charAt(pkStart+primaryKey.length());
						result=pkSide<=' ' || pkSide=='`' || pkSide==',' || pkSide==')';
					}
				}else{
					break;
				}
				pkOffset+=primaryKeyLength;
			}while(!result);
		}
		System.out.println(System.currentTimeMillis()-s);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			String upperSQL=origSQL.substring(17,35).toUpperCase();
		}
		System.out.println(System.currentTimeMillis()-s);
		
		s=System.currentTimeMillis();
		for(int i=0;i<100_0000;i++){
			String upperSQL=origSQL.toUpperCase();
		}
		System.out.println(System.currentTimeMillis()-s);
	}
}