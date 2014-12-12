package org.opencloudb.route.util;

import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.cache.LayerCachePool;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.route.SessionSQLPair;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.StringUtil;

/**
 * 从ServerRouterUtil中抽取的一些公用方法，路由解析工具类
 * @author wang.dw
 *
 */
public class RouterUtil {
	private static final Logger LOGGER = Logger.getLogger(RouterUtil.class);
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

	public static String removeSchema(String stmt, String schema) {
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
	public static RouteResultset routeToSingleNode(RouteResultset rrs,
			String dataNode, String stmt) {
		if (dataNode == null) {
			return rrs;
		}
		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);
		rrs.setNodes(nodes);
		return rrs;
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
	public static String getTableName(String stmt, int[] repPos) {
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
	 * 获取语句中前关键字位置和占位个数表名位置
	 * 
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author mycat
	 */
	public static int[] getCreateTablePos(String upStmt, int start) {
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
	 * 获取语句中前关键字位置和占位个数表名位置
	 * 
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author mycat
	 */
	public static int[] getSpecPos(String upStmt, int start) {
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
	 * 获取开始位置后的 LIKE、WHERE 位置 如果不含 LIKE、WHERE 则返回执行语句的长度
	 * 
	 * @param upStmt
	 *            执行sql
	 * @param start
	 *            开发位置
	 * @return int
	 * @author mycat
	 */
	public static int getSpecEndPos(String upStmt, int start) {
		int tabInd = upStmt.indexOf(" LIKE ", start);
		if (tabInd < 0) {
			tabInd = upStmt.indexOf(" WHERE ", start);
		}
		if (tabInd < 0) {
			return upStmt.length();
		}
		return tabInd;
	}
	
	public static boolean processWithMycatSeq(SystemConfig sysConfig,
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
	
	public static void processSQL(ServerConnection sc,SchemaConfig schema,String sql,int sqlType){
		MycatServer.getInstance().getSequnceProcessor().addNewSql(new SessionSQLPair(sc.getSession2(), schema, sql, sqlType));
	}
	
	public static boolean processInsert(SystemConfig sysConfig,
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
	
	public static boolean processInsert(ServerConnection sc,SchemaConfig schema,int sqlType,String origSQL,String tableName,String primaryKey){
		int firstLeftBracketIndex = origSQL.indexOf("(") + 1;
		int firstRightBracketIndex = origSQL.indexOf(")");

		boolean processedInsert=!isPKInFields(origSQL,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);
		if(processedInsert){
			processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);
		}
		return processedInsert;
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
	
	public static RouteResultset routeToMultiNode(boolean cache,RouteResultset rrs, Collection<String> dataNodes, String stmt) {
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
	
	public static void routeForTableMeta(RouteResultset rrs,
			SchemaConfig schema, String tableName, String sql) {
		String dataNode = null;
		if (schema.isNoSharding()) {//不分库的直接从schema中获取dataNode
			dataNode = schema.getDataNode();
		} else {
			dataNode = getMetaReadDataNode(schema, tableName);
		}

		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), sql);
		rrs.setNodes(nodes);
	}

	/**
	 * 根据标名随机获取一个节点
	 * 
	 * @param schema
	 *            数据库名
	 * @param table
	 *            表名
	 * @return 数据节点
	 * @author mycat
	 */
	private static String getMetaReadDataNode(SchemaConfig schema,
			String table) {
		// Table名字被转化为大写的，存储在schema
		table = table.toUpperCase();
		String dataNode = null;
		Map<String, TableConfig> tables = schema.getTables();
		TableConfig tc;
		if (tables != null && (tc = tables.get(table)) != null) {
			dataNode = tc.getRandomDataNode();
		}
		return dataNode;
	}
}
