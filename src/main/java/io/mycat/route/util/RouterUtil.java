package io.mycat.route.util;

import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.wall.spi.WallVisitorUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.mysql.nio.handler.FetchStoreNodeOfChildTableHandler;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.rule.RuleConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.SessionSQLPair;
import io.mycat.route.function.AbstractPartitionAlgorithm;
import io.mycat.route.function.SlotFunction;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.RouteCalculateUnit;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.ColumnRoutePair;
import io.mycat.sqlengine.mpp.LoadData;
import io.mycat.util.StringUtil;

/**
 * 从ServerRouterUtil中抽取的一些公用方法，路由解析工具类
 * @author wang.dw
 *
 */
public class RouterUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(RouterUtil.class);

	/**
	 * 移除执行语句中的数据库名
	 *
	 * @param stmt		 执行语句
	 * @param schema  	数据库名
	 * @return 			执行语句
	 * @author mycat
	 *
	 * @modification 修正移除schema的方法
	 * @date 2016/12/29
	 * @modifiedBy Hash Zhang
	 *
	 */
	public static String removeSchema(String stmt, String schema) {
		final String upStmt = stmt.toUpperCase();
		final String upSchema = schema.toUpperCase() + ".";
		final String upSchema2 = new StringBuilder("`").append(schema.toUpperCase()).append("`.").toString();
		int strtPos = 0;
		int indx = 0;

		int indx1 = upStmt.indexOf(upSchema, strtPos);
		int indx2 = upStmt.indexOf(upSchema2, strtPos);
		boolean flag = indx1 < indx2 ? indx1 == -1 : indx2 != -1;
		indx = !flag ? indx1 > 0 ? indx1 : indx2 : indx2 > 0 ? indx2 : indx1;
		if (indx < 0) {
			return stmt;
		}

		int firstE = upStmt.indexOf("'");
		int endE = upStmt.lastIndexOf("'");

		StringBuilder sb = new StringBuilder();
		while (indx > 0) {
			sb.append(stmt.substring(strtPos, indx));

			if (flag) {
				strtPos = indx + upSchema2.length();
			} else {
				strtPos = indx + upSchema.length();
			}
			if (indx > firstE && indx < endE && countChar(stmt, indx) % 2 == 1) {
				sb.append(stmt.substring(indx, indx + schema.length() + 1));
			}
			indx1 = upStmt.indexOf(upSchema, strtPos);
			indx2 = upStmt.indexOf(upSchema2, strtPos);
			flag = indx1 < indx2 ? indx1 == -1 : indx2 != -1;
			indx = !flag ? indx1 > 0 ? indx1 : indx2 : indx2 > 0 ? indx2 : indx1;
		}
		sb.append(stmt.substring(strtPos));
		return sb.toString();
	}

	private static int countChar(String sql,int end)
	{
		int count=0;
		boolean skipChar = false;
		for (int i = 0; i < end; i++) {
			if(sql.charAt(i)=='\'' && !skipChar) {
				count++;
				skipChar = false;
			}else if( sql.charAt(i)=='\\'){
				skipChar = true;
			}else{
				skipChar = false;
			}
		}
		return count;
	}

	/**
	 * 获取第一个节点作为路由
	 *
	 * @param rrs		          数据路由集合
	 * @param dataNode  	数据库所在节点
	 * @param stmt   		执行语句
	 * @return 				数据路由集合
	 *
	 * @author mycat
	 */
	public static RouteResultset routeToSingleNode(RouteResultset rrs,
			String dataNode, String stmt) {
		if (dataNode == null) {
			return rrs;
		}
		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);//rrs.getStatement()
		nodes[0].setSource(rrs);
		rrs.setNodes(nodes);
		rrs.setFinishedRoute(true);
		if(rrs.getDataNodeSlotMap().containsKey(dataNode)){
			nodes[0].setSlot(rrs.getDataNodeSlotMap().get(dataNode));
		}
		if (rrs.getCanRunInReadDB() != null) {
			nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
		}
		if(rrs.getRunOnSlave() != null){
			nodes[0].setRunOnSlave(rrs.getRunOnSlave());
		}

		return rrs;
	}



	/**
	 * 修复DDL路由
	 *
	 * @return RouteResultset
	 * @author aStoneGod
	 */
	public static RouteResultset routeToDDLNode(RouteResultset rrs, int sqlType, String stmt,SchemaConfig schema) throws SQLSyntaxErrorException {
		stmt = getFixedSql(stmt);
		String tablename = "";
		final String upStmt = stmt.toUpperCase();
		if(upStmt.startsWith("CREATE")){
			if (upStmt.contains("CREATE INDEX ") || upStmt.contains("CREATE UNIQUE INDEX ")){
				tablename = RouterUtil.getTableName(stmt, RouterUtil.getCreateIndexPos(upStmt, 0));
			}else {
				tablename = RouterUtil.getTableName(stmt, RouterUtil.getCreateTablePos(upStmt, 0));
			}
		}else if(upStmt.startsWith("DROP")){
			if (upStmt.contains("DROP INDEX ")){
				tablename = RouterUtil.getTableName(stmt, RouterUtil.getDropIndexPos(upStmt, 0));
			}else {
				tablename = RouterUtil.getTableName(stmt, RouterUtil.getDropTablePos(upStmt, 0));
			}
		}else if(upStmt.startsWith("ALTER")){
			tablename = RouterUtil.getTableName(stmt, RouterUtil.getAlterTablePos(upStmt, 0));
		}else if (upStmt.startsWith("TRUNCATE")){
			tablename = RouterUtil.getTableName(stmt, RouterUtil.getTruncateTablePos(upStmt, 0));
		}
		tablename = tablename.toUpperCase();

		if (schema.getTables().containsKey(tablename)){
			if(ServerParse.DDL==sqlType){
				List<String> dataNodes = new ArrayList<>();
				Map<String, TableConfig> tables = schema.getTables();
				TableConfig tc=tables.get(tablename);
				if (tables != null && (tc  != null)) {
					dataNodes = tc.getDataNodes();
				}
				boolean isSlotFunction= tc.getRule() != null && tc.getRule().getRuleAlgorithm() instanceof SlotFunction;
				Iterator<String> iterator1 = dataNodes.iterator();
				int nodeSize = dataNodes.size();
				RouteResultsetNode[] nodes = new RouteResultsetNode[nodeSize];
				if(isSlotFunction){
					stmt=changeCreateTable(schema,tablename,stmt);
				}
				for(int i=0;i<nodeSize;i++){
					String name = iterator1.next();
					nodes[i] = new RouteResultsetNode(name, sqlType, stmt);
					nodes[i].setSource(rrs);
					if(rrs.getDataNodeSlotMap().containsKey(name)){
						nodes[i].setSlot(rrs.getDataNodeSlotMap().get(name));
					}  else if(isSlotFunction){
						nodes[i].setSlot(-1);
					}
				}
				rrs.setNodes(nodes);
			}
			return rrs;
		}else if(schema.getDataNode()!=null){		//默认节点ddl
			RouteResultsetNode[] nodes = new RouteResultsetNode[1];
			nodes[0] = new RouteResultsetNode(schema.getDataNode(), sqlType, stmt);
			nodes[0].setSource(rrs);
			rrs.setNodes(nodes);
			return rrs;
		}
		//both tablename and defaultnode null
		LOGGER.error("table not in schema----"+tablename);
		throw new SQLSyntaxErrorException("op table not in schema----"+tablename);
	}

	private  static String changeCreateTable(SchemaConfig schema,String tableName,String sql) {
		if (schema.getTables().containsKey(tableName)) {
			MySqlStatementParser parser = new MySqlStatementParser(sql);
			SQLStatement insertStatement = parser.parseStatement();
			if (insertStatement instanceof MySqlCreateTableStatement) {
				TableConfig tableConfig = schema.getTables().get(tableName);
				AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
				if (algorithm instanceof SlotFunction) {
					SQLColumnDefinition column = new SQLColumnDefinition();
					column.setDataType(new SQLCharacterDataType("int"));
					column.setName(new SQLIdentifierExpr("_slot"));
					column.setComment(new SQLCharExpr("自动迁移算法slot,禁止修改"));
					((SQLCreateTableStatement) insertStatement).getTableElementList().add(column);
					return insertStatement.toString();

				}
			}

		}
		return sql;
	}

	/**
	 * 处理SQL
	 *
	 * @param stmt   执行语句
	 * @return 		 处理后SQL
	 * @author AStoneGod
	 */
	public static String getFixedSql(String stmt){
		stmt = stmt.replaceAll("\r\n", " "); //对于\r\n的字符 用 空格处理 rainbow
		return stmt = stmt.trim(); //.toUpperCase();
	}

	/**
	 * 获取table名字
	 *
	 * @param stmt  	执行语句
	 * @param repPos	开始位置和位数
	 * @return 表名
	 * @author AStoneGod
	 */
	public static String getTableName(String stmt, int[] repPos) {
		int startPos = repPos[0];
		int secInd = stmt.indexOf(' ', startPos + 1);
		if (secInd < 0) {
			secInd = stmt.length();
		}
		int thiInd = stmt.indexOf('(',secInd+1);
		if (thiInd < 0) {
			thiInd = stmt.length();
		}
		repPos[1] = secInd;
		String tableName = "";
		if (stmt.toUpperCase().startsWith("DESC")||stmt.toUpperCase().startsWith("DESCRIBE")){
			tableName = stmt.substring(startPos, thiInd).trim();
		}else {
			tableName = stmt.substring(secInd, thiInd).trim();
		}

		//ALTER TABLE
		if (tableName.contains(" ")){
			tableName = tableName.substring(0,tableName.indexOf(" "));
		}
		int ind2 = tableName.indexOf('.');
		if (ind2 > 0) {
			tableName = tableName.substring(ind2 + 1);
		}
		return tableName;
	}


	/**
	 * 获取show语句table名字
	 *
	 * @param stmt	        执行语句
	 * @param repPos   开始位置和位数
	 * @return 表名
	 * @author AStoneGod
	 */
	public static String getShowTableName(String stmt, int[] repPos) {
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
	 * @param upStmt     执行语句
	 * @param start      开始位置
	 * @return int[]	  关键字位置和占位个数
	 *
	 * @author mycat
	 *
	 * @modification 修改支持语句中包含“IF NOT EXISTS”的情况
	 * @date 2016/12/8
	 * @modifiedBy Hash Zhang
	 */
	public static int[] getCreateTablePos(String upStmt, int start) {
		String token1 = "CREATE ";
		String token2 = " TABLE ";
		String token3 = " EXISTS ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd1 = upStmt.indexOf(token2, start);
		int tabInd2 = upStmt.indexOf(token3, tabInd1);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd2 > 0 && tabInd2 > createInd) {
			return new int[] { tabInd2, token3.length() };
		} else if(createInd >= 0 && tabInd1 > 0 && tabInd1 > createInd) {
			return new int[] { tabInd1, token2.length() };
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
	 * @author aStoneGod
	 */
	public static int[] getCreateIndexPos(String upStmt, int start) {
		String token1 = "CREATE ";
		String token2 = " INDEX ";
		String token3 = " ON ";
		int createInd = upStmt.indexOf(token1, start);
		int idxInd = upStmt.indexOf(token2, start);
		int onInd = upStmt.indexOf(token3, start);
		// 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
		if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
			return new int[] {onInd , token3.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取ALTER语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt   执行语句
	 * @param start    开始位置
	 * @return int[]   关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getAlterTablePos(String upStmt, int start) {
		String token1 = "ALTER ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取DROP语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt 	执行语句
	 * @param start  	开始位置
	 * @return int[]	关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getDropTablePos(String upStmt, int start) {
		//增加 if exists判断
		if(upStmt.contains("EXISTS")){
			String token1 = "IF ";
			String token2 = " EXISTS ";
			int ifInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);
			if (ifInd >= 0 && tabInd > 0 && tabInd > ifInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}else {
			String token1 = "DROP ";
			String token2 = " TABLE ";
			int createInd = upStmt.indexOf(token1, start);
			int tabInd = upStmt.indexOf(token2, start);

			if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
				return new int[] { tabInd, token2.length() };
			} else {
				return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
			}
		}
	}


	/**
	 * 获取DROP语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt
	 *            执行语句
	 * @param start
	 *            开始位置
	 * @return int[]关键字位置和占位个数
	 * @author aStoneGod
	 */

	public static int[] getDropIndexPos(String upStmt, int start) {
		String token1 = "DROP ";
		String token2 = " INDEX ";
		String token3 = " ON ";
		int createInd = upStmt.indexOf(token1, start);
		int idxInd = upStmt.indexOf(token2, start);
		int onInd = upStmt.indexOf(token3, start);
		// 既包含CREATE又包含INDEX，且CREATE关键字在INDEX关键字之前, 且包含ON...
		if (createInd >= 0 && idxInd > 0 && idxInd > createInd && onInd > 0 && onInd > idxInd) {
			return new int[] {onInd , token3.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取TRUNCATE语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt    执行语句
	 * @param start     开始位置
	 * @return int[]	关键字位置和占位个数
	 * @author aStoneGod
	 */
	public static int[] getTruncateTablePos(String upStmt, int start) {
		String token1 = "TRUNCATE ";
		String token2 = " TABLE ";
		int createInd = upStmt.indexOf(token1, start);
		int tabInd = upStmt.indexOf(token2, start);
		// 既包含CREATE又包含TABLE，且CREATE关键字在TABLE关键字之前
		if (createInd >= 0 && tabInd > 0 && tabInd > createInd) {
			return new int[] { tabInd, token2.length() };
		} else {
			return new int[] { -1, token2.length() };// 不满足条件时，只关注第一个返回值为-1，第二个任意
		}
	}

	/**
	 * 获取语句中前关键字位置和占位个数表名位置
	 *
	 * @param upStmt   执行语句
	 * @param start    开始位置
	 * @return int[]   关键字位置和占位个数
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
	 * @param upStmt   执行sql
	 * @param start    开始位置
	 * @return int
	 * @author mycat
	 */
	public static int getSpecEndPos(String upStmt, int start) {
		int tabInd = upStmt.toUpperCase().indexOf(" LIKE ", start);
		if (tabInd < 0) {
			tabInd = upStmt.toUpperCase().indexOf(" WHERE ", start);
		}
		if (tabInd < 0) {
			return upStmt.length();
		}
		return tabInd;
	}

	public static boolean processWithMycatSeq(SchemaConfig schema, int sqlType,
			String origSQL, ServerConnection sc) {
		// check if origSQL is with global sequence
		// @micmiu it is just a simple judgement
		//对应本地文件配置方式：insert into table1(id,name) values(next value for MYCATSEQ_GLOBAL,‘test’);
		// edit by dingw,增加mycatseq_ 兼容，因为ServerConnection的373行，进行路由计算时，将原始语句全部转换为小写
		if (origSQL.indexOf(" MYCATSEQ_") != -1 || origSQL.indexOf("mycatseq_") != -1) {
			processSQL(sc,schema,origSQL,sqlType);
			return true;
		}
		return false;
	}

	public static void processSQL(ServerConnection sc,SchemaConfig schema,String sql,int sqlType){
//		int sequenceHandlerType = MycatServer.getInstance().getConfig().getSystem().getSequnceHandlerType();
		final SessionSQLPair sessionSQLPair = new SessionSQLPair(sc.getSession2(), schema, sql, sqlType);
//      modify by yanjunli  序列获取修改为多线程方式。使用分段锁方式,一个序列一把锁。  begin
//		MycatServer.getInstance().getSequnceProcessor().addNewSql(sessionSQLPair);
		MycatServer.getInstance().getSequenceExecutor().execute(new Runnable() {
			@Override
			public void run() {
				MycatServer.getInstance().getSequnceProcessor().executeSeq(sessionSQLPair);
			}
		});
//      modify   序列获取修改为多线程方式。使用分段锁方式,一个序列一把锁。  end
//		}
	}

	public static boolean processInsert(SchemaConfig schema, int sqlType,
			String origSQL, ServerConnection sc) throws SQLNonTransientException {
		String tableName = StringUtil.getTableName(origSQL).toUpperCase();
		TableConfig tableConfig = schema.getTables().get(tableName);
		boolean processedInsert=false;
		//判断是有自增字段
		if (null != tableConfig && tableConfig.isAutoIncrement()) {
			String primaryKey = tableConfig.getPrimaryKey();
			processedInsert=processInsert(sc,schema,sqlType,origSQL,tableName,primaryKey);
		}
		return processedInsert;
	}
	/*
	 *  找到返回主键的的位置
	 *  找不到返回 -1
	 * */
	private static int isPKInFields(String origSQL,String primaryKey,int firstLeftBracketIndex,int firstRightBracketIndex){

		if (primaryKey == null) {
			throw new RuntimeException("please make sure the primaryKey's config is not null in schemal.xml");
		}

		boolean isPrimaryKeyInFields = false;
		int  pkStart = 0;
		String upperSQL = origSQL.substring(firstLeftBracketIndex, firstRightBracketIndex + 1).toUpperCase();
		for (int pkOffset = 0, primaryKeyLength = primaryKey.length();;) {
			pkStart = upperSQL.indexOf(primaryKey, pkOffset);
			if (pkStart >= 0 && pkStart < firstRightBracketIndex) {
				char pkSide = upperSQL.charAt(pkStart - 1);
				if (pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == '(') {
					pkSide = upperSQL.charAt(pkStart + primaryKey.length());
					isPrimaryKeyInFields = pkSide <= ' ' || pkSide == '`' || pkSide == ',' || pkSide == ')';
				}
				if (isPrimaryKeyInFields) {
					break;
				}
				pkOffset = pkStart + primaryKeyLength;
			} else {
				break;
			}
		}
		if (isPrimaryKeyInFields) {
			return firstLeftBracketIndex + pkStart;
		} else {
			return  -1;
		}

	}

	public static boolean processInsert(ServerConnection sc,SchemaConfig schema,
			int sqlType,String origSQL,String tableName,String primaryKey) throws SQLNonTransientException {

		int firstLeftBracketIndex = origSQL.indexOf("(");
		int firstRightBracketIndex = origSQL.indexOf(")");
		String upperSql = origSQL.toUpperCase();
		int valuesIndex = upperSql.indexOf("VALUES");
		int selectIndex = upperSql.indexOf("SELECT");
		int fromIndex = upperSql.indexOf("FROM");
		//屏蔽insert into table1 select * from table2语句
		if(firstLeftBracketIndex < 0) {
			String msg = "invalid sql:" + origSQL;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		//屏蔽批量插入
		if(selectIndex > 0 &&fromIndex>0&&selectIndex>firstRightBracketIndex&&valuesIndex<0) {
			String msg = "multi insert not provided" ;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		//插入语句必须提供列结构，因为MyCat默认对于表结构无感知
		if(valuesIndex + "VALUES".length() <= firstLeftBracketIndex) {
			throw new SQLSyntaxErrorException("insert must provide ColumnList");
		}
		Object[] vauleArrayAndSuffixStr = parseSqlValueArrayAndSuffixStr(origSQL , valuesIndex);
		List<List<String>> vauleArray = (List<List<String>>) vauleArrayAndSuffixStr[0];
		String suffixStr = null;
		if (vauleArrayAndSuffixStr.length > 1) {
			suffixStr = (String) vauleArrayAndSuffixStr[1];
		}
		//两种情况处理 1 有主键的 id ,但是值为null 进行改下
		//            2 没有主键的 需要插入 进行改写

		//如果主键不在插入语句的fields中，则需要进一步处理
		boolean processedInsert= false;
		int pkStart = isPKInFields(origSQL,primaryKey,firstLeftBracketIndex,firstRightBracketIndex);


		if(pkStart == -1){
			processedInsert = true;
			handleBatchInsert(sc, schema, sqlType,origSQL, valuesIndex, tableName, primaryKey, vauleArray, suffixStr);
		} else {
			//判断 主键id的值是否为null
			if(pkStart != -1) {
				String subPrefix = origSQL.substring(0, pkStart);
				char c;
				int pkIndex = 0;
				for(int index = 0, len = subPrefix.length(); index < len; index++) {
					c = subPrefix.charAt(index);
					if(c == ',') {
						pkIndex ++;
					}
				}
				processedInsert  = handleBatchInsertWithPK(sc, schema, sqlType,origSQL, valuesIndex, tableName, primaryKey, vauleArray, suffixStr, pkIndex);
			}
		}
		return processedInsert;
	}

	private static boolean handleBatchInsertWithPK(ServerConnection sc, SchemaConfig schema, int sqlType,
			String origSQL, int valuesIndex, String tableName, String primaryKey, List<List<String>> vauleList,
			String suffixStr, int pkIndex) {
		boolean processedInsert = false;
//	  	final String pk = "\\("+primaryKey+",";
		final String mycatSeqPrefix = "next value for MYCATSEQ_"+tableName.toUpperCase() ;

		/*"VALUES".length() ==6 */
		String prefix = origSQL.substring(0, valuesIndex + 6);
//

		StringBuilder sb = new StringBuilder("");
		for(List<String> list : vauleList) {
			sb.append("(");
			String pkValue = list.get(pkIndex).trim().toLowerCase();
			//null值替换为 next value for MYCATSEQ_tableName
			if("null".equals(pkValue.trim())) {
				list.set(pkIndex, mycatSeqPrefix);
				processedInsert = true;
			}
			for(String val : list) {
				sb.append(val).append(",");
			}
			sb.setCharAt(sb.length() - 1, ')');
			sb.append(",");
		}
		sb.setCharAt(sb.length() - 1, ' ');
		if (suffixStr != null) {
			sb.append(suffixStr);
		}
		if(processedInsert) {
			processSQL(sc, schema,prefix+sb.toString(), sqlType);
		}
		return processedInsert;
	}

	public static List<String> handleBatchInsert(String origSQL, int valuesIndex) {
		List<String> handledSQLs = new LinkedList<>();
		String prefix = origSQL.substring(0, valuesIndex + "VALUES".length());
		String values = origSQL.substring(valuesIndex + "VALUES".length());
		int flag = 0;
		StringBuilder currentValue = new StringBuilder();
		currentValue.append(prefix);
		for (int i = 0; i < values.length(); i++) {
			char j = values.charAt(i);
			if (j == '(' && flag == 0) {
				flag = 1;
				currentValue.append(j);
			} else if (j == '\"' && flag == 1) {
				flag = 2;
				currentValue.append(j);
			} else if (j == '\'' && flag == 1) {
				flag = 3;
				currentValue.append(j);
			} else if (j == '\\' && flag == 2) {
				flag = 4;
				currentValue.append(j);
			} else if (j == '\\' && flag == 3) {
				flag = 5;
				currentValue.append(j);
			} else if (flag == 4) {
				flag = 2;
				currentValue.append(j);
			} else if (flag == 5) {
				flag = 3;
				currentValue.append(j);
			} else if (j == '\"' && flag == 2) {
				flag = 1;
				currentValue.append(j);
			} else if (j == '\'' && flag == 3) {
				flag = 1;
				currentValue.append(j);
			} else if (j == ')' && flag == 1) {
				flag = 0;
				currentValue.append(j);
				handledSQLs.add(currentValue.toString());
				currentValue = new StringBuilder();
				currentValue.append(prefix);
			} else if (j == ',' && flag == 0) {
				continue;
			} else {
				currentValue.append(j);
			}
		}
		return handledSQLs;
	}
	/**
	 * 对于插入的sql : "insert into hotnews(title,name) values('test1',\"name\"),('(test)',\"(test)\"),('\\\"',\"\\'\"),(\")\",\"\\\"\\')\")"：
	 *  需要返回结果：
	 *[[ 'test1', "name"],
	 *	['(test)', "(test)"],
	 *	['\"', "\'"],
	 *	[")", "\"\')"],
	 *	[ 1,  null]
	 * 值结果的解析
	 */
	public static Object[] parseSqlValueArrayAndSuffixStr(String origSQL, int valuesIndex) {
		List<List<String>> valueArray = new ArrayList<>();
		String valuesAndSuffixStr = origSQL.substring(valuesIndex + 6);// 6 values 长度为6
		int pos = 0 ;
		int flag  = 4;
		int len = valuesAndSuffixStr.length();
		StringBuilder currentValue = new StringBuilder();
//        int colNum = 2; //
		char c ;
		List<String> curList = new ArrayList<>();
		int parenCount = 0;
		for( ;pos < len; pos ++) {
			c = valuesAndSuffixStr.charAt(pos);
			if (flag == 1  || flag == 2) {
				currentValue.append(c);
				if (c == '\\') {
					char nextCode = valuesAndSuffixStr.charAt(pos + 1);
					if (nextCode == '\'' || nextCode == '\"') {
						currentValue.append(nextCode);
						pos++;
						continue;
					}
				}
				if (c == '\"' && flag == 1) {
					flag = 0;
					continue;
				}
				if (c == '\'' && flag == 2) {
					flag = 0;
					continue;
				}
			} else if (flag == 5) {
				currentValue.append(c);
				if (c == '(') {
					parenCount++;
				} else if (c == ')') {
					parenCount--;
				}
				if (parenCount == 0) {
					flag = 0;
				}
			} else if (c == '\"'){
				currentValue.append(c);
				flag = 1;
			} else if (c == '\'') {
				currentValue.append(c);
				flag = 2;
			} else if (c == '(') {
				if (flag == 4) {
					curList = new ArrayList<>();
					flag = 0;
				} else {
					currentValue.append(c);
					flag = 5;
					parenCount++;
				}
			} else if (flag == 4) {
				if (c == 'o' || c == 'O') {
					String suffixStr = valuesAndSuffixStr.substring(pos);
					return new Object[]{valueArray, suffixStr};
				}
				continue;
			} else if (c == ',') {
//                System.out.println(currentValue);
				curList.add(currentValue.toString());
				currentValue.delete(0, currentValue.length());
			} else if (c == ')'){
				flag = 4;
//                System.out.println(currentValue);
				curList.add(currentValue.toString());
				currentValue.delete(0, currentValue.length());
				valueArray.add(curList);
			}  else {
				currentValue.append(c);
			}
		}
		return new Object[]{valueArray};
	}
	/**
	 * 对于主键不在插入语句的fields中的SQL，需要改写。比如hotnews主键为id，插入语句为：
	 * insert into hotnews(title) values('aaa');
	 * 需要改写成：
	 * insert into hotnews(id, title) values(next value for MYCATSEQ_hotnews,'aaa');
	 */
	public static void handleBatchInsert(ServerConnection sc, SchemaConfig schema,
			int sqlType,String origSQL, int valuesIndex,String tableName, String primaryKey , List<List<String>> vauleList, String suffixStr) {

		final String pk = "\\("+primaryKey+",";
		final String mycatSeqPrefix = "(next value for MYCATSEQ_"+tableName.toUpperCase()+"";

		/*"VALUES".length() ==6 */
		String prefix = origSQL.substring(0, valuesIndex + 6);
//
		prefix = prefix.replaceFirst("\\(", pk);

		StringBuilder sb = new StringBuilder("");
		for(List<String> list : vauleList) {
			sb.append(mycatSeqPrefix);
			for(String val : list) {
				sb.append(",").append(val);
			}
			sb.append("),");
		}
		sb.setCharAt(sb.length() - 1, ' ');
		if (suffixStr != null) {
			sb.append(suffixStr);
		}
		processSQL(sc, schema,prefix+sb.toString(), sqlType);
	}
//	  /**
//	  * 对于主键不在插入语句的fields中的SQL，需要改写。比如hotnews主键为id，插入语句为：
//	  * insert into hotnews(title) values('aaa');
//	  * 需要改写成：
//	  * insert into hotnews(id, title) values(next value for MYCATSEQ_hotnews,'aaa');
//	  */
//    public static void handleBatchInsert(ServerConnection sc, SchemaConfig schema,
//            int sqlType,String origSQL, int valuesIndex,String tableName, String primaryKey) {
//
//    	final String pk = "\\("+primaryKey+",";
//        final String mycatSeqPrefix = "(next value for MYCATSEQ_"+tableName.toUpperCase()+",";
//
//    	/*"VALUES".length() ==6 */
//        String prefix = origSQL.substring(0, valuesIndex + 6);
//        String values = origSQL.substring(valuesIndex + 6);
//
//        prefix = prefix.replaceFirst("\\(", pk);
//        values = values.replaceFirst("\\(", mycatSeqPrefix);
//        values =Pattern.compile(",\\s*\\(").matcher(values).replaceAll(","+mycatSeqPrefix);
//        processSQL(sc, schema,prefix+values, sqlType);
//    }


	public static RouteResultset routeToMultiNode(boolean cache,RouteResultset rrs, Collection<String> dataNodes, String stmt) {
		RouteResultsetNode[] nodes = new RouteResultsetNode[dataNodes.size()];
		int i = 0;
		RouteResultsetNode node;
		for (String dataNode : dataNodes) {
			node = new RouteResultsetNode(dataNode, rrs.getSqlType(), stmt);
			node.setSource(rrs);
			if(rrs.getDataNodeSlotMap().containsKey(dataNode)){
				node.setSlot(rrs.getDataNodeSlotMap().get(dataNode));
			}
			if (rrs.getCanRunInReadDB() != null) {
				node.setCanRunInReadDB(rrs.getCanRunInReadDB());
			}
			if(rrs.getRunOnSlave() != null){
				nodes[0].setRunOnSlave(rrs.getRunOnSlave());
			}
			nodes[i++] = node;
		}
		rrs.setCacheAble(cache);
		rrs.setNodes(nodes);
		return rrs;
	}

	public static RouteResultset routeToMultiNode(boolean cache, RouteResultset rrs, Collection<String> dataNodes,
			String stmt, boolean isGlobalTable) {

		rrs = routeToMultiNode(cache, rrs, dataNodes, stmt);
		rrs.setGlobalTable(isGlobalTable);
		return rrs;
	}

	public static void routeForTableMeta(RouteResultset rrs,
			SchemaConfig schema, String tableName, String sql) {
		String dataNode = null;
		if (isNoSharding(schema,tableName)) {//不分库的直接从schema中获取dataNode
			dataNode = schema.getDataNode();
		} else {
			dataNode = getMetaReadDataNode(schema, tableName);
		}

		RouteResultsetNode[] nodes = new RouteResultsetNode[1];
		nodes[0] = new RouteResultsetNode(dataNode, rrs.getSqlType(), sql);
		nodes[0].setSource(rrs);
		if(rrs.getDataNodeSlotMap().containsKey(dataNode)){
			nodes[0].setSlot(rrs.getDataNodeSlotMap().get(dataNode));
		}
		if (rrs.getCanRunInReadDB() != null) {
			nodes[0].setCanRunInReadDB(rrs.getCanRunInReadDB());
		}
		if(rrs.getRunOnSlave() != null){
			nodes[0].setRunOnSlave(rrs.getRunOnSlave());
		}
		rrs.setNodes(nodes);
	}

	/**
	 * 根据表名随机获取一个节点
	 *
	 * @param schema     数据库名
	 * @param table      表名
	 * @return 			  数据节点
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
			dataNode = getAliveRandomDataNode(tc);
		}
		return dataNode;
	}

	/**
	 * 解决getRandomDataNode方法获取错误节点的问题.
	 * @param tc
	 * @return
	 */
	private static String getAliveRandomDataNode(TableConfig tc) {
		List<String> randomDns = (List<String>)tc.getDataNodes().clone();

		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		if (mycatConfig != null) {
			Collections.shuffle(randomDns);
			for (String randomDn : randomDns) {
				PhysicalDBNode physicalDBNode = mycatConfig.getDataNodes().get(randomDn);
				if (physicalDBNode != null) {
					if (physicalDBNode.getDbPool().getSource().isAlive()) {
						for (PhysicalDBPool pool : MycatServer.getInstance().getConfig().getDataHosts().values()) {
							PhysicalDatasource source = pool.getSource();
							if (source.getHostConfig().containDataNode(randomDn) && pool.getSource().isAlive()) {
								return randomDn;
							}
						}
					}
				}
			}
		}

		// all fail return default
		return tc.getRandomDataNode();
	}

	@Deprecated
	private static String getRandomDataNode(TableConfig tc) {
		//写节点不可用，意味着读节点也不可用。
		//直接使用下一个 dataHost
		String randomDn = tc.getRandomDataNode();
		MycatConfig mycatConfig = MycatServer.getInstance().getConfig();
		if (mycatConfig != null) {
			PhysicalDBNode physicalDBNode = mycatConfig.getDataNodes().get(randomDn);
			if (physicalDBNode != null) {
				if (physicalDBNode.getDbPool().getSource().isAlive()) {
					for (PhysicalDBPool pool : MycatServer.getInstance()
							.getConfig()
							.getDataHosts()
							.values()) {
						if (pool.getSource().getHostConfig().containDataNode(randomDn)) {
							continue;
						}

						if (pool.getSource().isAlive()) {
							return pool.getSource().getHostConfig().getRandomDataNode();
						}
					}
				}
			}
		}

		//all fail return default
		return randomDn;
	}

	/**
	 * 根据 ER分片规则获取路由集合
	 *
	 * @param stmt            执行的语句
	 * @param rrs      		     数据路由集合
	 * @param tc	      	     表实体
	 * @param joinKeyVal      连接属性
	 * @return RouteResultset(数据路由集合)	 *
	 * @throws SQLNonTransientException，IllegalShardingColumnValueException
	 * @author mycat
	 */

	public static RouteResultset routeByERParentKey(ServerConnection sc,SchemaConfig schema,
			int sqlType,String stmt,
			RouteResultset rrs, TableConfig tc, String joinKeyVal)
			throws SQLNonTransientException {

		// only has one parent level and ER parent key is parent
		// table's partition key
		if (tc.isSecondLevel()
				//判断是否为二级子表（父表不再有父表）
				&& tc.getParentTC().getPartitionColumn()
				.equals(tc.getParentKey())) { // using
			// parent
			// rule to
			// find
			// datanode
			Set<ColumnRoutePair> parentColVal = new HashSet<ColumnRoutePair>(1);
			ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
			parentColVal.add(pair);
			Set<String> dataNodeSet = ruleCalculate(tc.getParentTC(), parentColVal,rrs.getDataNodeSlotMap());
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
			return RouterUtil.routeToSingleNode(rrs, dn, stmt);
		}
		return null;
	}

	/**
	 * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
	 */
	public static Set<String> ruleByJoinValueCalculate(RouteResultset rrs, TableConfig tc,
			Set<ColumnRoutePair> colRoutePairSet) throws SQLNonTransientException {

		String joinValue = "";

		if(colRoutePairSet.size() > 1) {
			LOGGER.warn("joinKey can't have multi Value");
		} else {
			Iterator<ColumnRoutePair> it = colRoutePairSet.iterator();
			ColumnRoutePair joinCol = it.next();
			joinValue = joinCol.colValue;
		}

		Set<String> retNodeSet = new LinkedHashSet<String>();

		Set<String> nodeSet;
		if (tc.isSecondLevel()
				&& tc.getParentTC().getPartitionColumn()
				.equals(tc.getParentKey())) { // using
			// parent
			// rule to
			// find
			// datanode

			nodeSet = ruleCalculate(tc.getParentTC(),colRoutePairSet,rrs.getDataNodeSlotMap());
			if (nodeSet.isEmpty()) {
				throw new SQLNonTransientException(
						"parent key can't find  valid datanode ,expect 1 but found: "
								+ nodeSet.size());
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  "
						+ nodeSet + " sql :" + rrs.getStatement());
			}
			retNodeSet.addAll(nodeSet);

//			for(ColumnRoutePair pair : colRoutePairSet) {
//				nodeSet = ruleCalculate(tc.getParentTC(),colRoutePairSet);
//				if (nodeSet.isEmpty() || nodeSet.size() > 1) {//an exception would be thrown, if sql was executed on more than on sharding
//					throw new SQLNonTransientException(
//							"parent key can't find  valid datanode ,expect 1 but found: "
//									+ nodeSet.size());
//				}
//				String dn = nodeSet.iterator().next();
//				if (LOGGER.isDebugEnabled()) {
//					LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  "
//							+ dn + " sql :" + rrs.getStatement());
//				}
//				retNodeSet.addAll(nodeSet);
//			}
			return retNodeSet;
		} else {
			retNodeSet.addAll(tc.getParentTC().getDataNodes());
		}

		return retNodeSet;
	}


	/**
	 * @return dataNodeIndex -&gt; [partitionKeysValueTuple+]
	 */
	public static Set<String> ruleCalculate(TableConfig tc,
			Set<ColumnRoutePair> colRoutePairSet,Map<String,Integer>   dataNodeSlotMap)  {
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
					if(algorithm instanceof SlotFunction) {
						dataNodeSlotMap.put(dataNode,((SlotFunction) algorithm).slotValue());
					}
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
							if(algorithm instanceof SlotFunction) {
								dataNodeSlotMap.put(dataNode,((SlotFunction) algorithm).slotValue());
							}
							routeNodeSet.add(dataNode);
						}
					}
				}
			}

		}
		return routeNodeSet;
	}

	/**
	 * 多表路由
	 */
	public static RouteResultset tryRouteForTables(SchemaConfig schema, DruidShardingParseInfo ctx,
			RouteCalculateUnit routeUnit, RouteResultset rrs, boolean isSelect, LayerCachePool cachePool)
			throws SQLNonTransientException {

		List<String> tables = ctx.getTables();

		//每个表对应的路由映射
		Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();

		//为全局表和单库表找路由
		for(String tableName : tables) {

			TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());

			if(tableConfig == null) {
				//add 如果表读取不到则先将表名从别名中读取转化后再读取
				String alias = ctx.getTableAliasMap().get(tableName);
				if(!StringUtil.isEmpty(alias)){
					tableConfig = schema.getTables().get(alias.toUpperCase());
				}

				if(tableConfig == null){
					String msg = "can't find table define in schema "+ tableName + " schema:" + schema.getName();
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}

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

			if(tableConfig.getDistTables().size() > 0) {
				Map<String, List<String>> subTablesmap = rrs.getSubTableMaps();
				if (subTablesmap == null) {
					subTablesmap = Maps.newHashMap();
					rrs.setSubTableMaps(subTablesmap);
				}

				subTablesmap.put(tableName.toUpperCase(), tableConfig.getDistTables());
			}
		}

		if(schema.isNoSharding()||(tables.size() >= 1&&isNoSharding(schema,tables.get(0)))) {
			return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
		}

		//只有一个表的
		if(tables.size() == 1) {
			return RouterUtil.tryRouteForOneTable(schema, ctx, routeUnit, tables.get(0), rrs, isSelect, cachePool);
		}

		Set<String> retNodesSet = new HashSet<String>();

		//分库解析信息不为空
		Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions = routeUnit.getTablesAndConditions();
		if(tablesAndConditions != null && tablesAndConditions.size() > 0) {
			//为分库表找路由
			RouterUtil.findRouteWithcConditionsForTables(schema, rrs, tablesAndConditions, tablesRouteMap, ctx.getSql(), cachePool, isSelect);
			if(rrs.isFinishedRoute()) {
				return rrs;
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
			String tableName = tables.get(0);
			TableConfig tableConfig = schema.getTables().get(tableName.toUpperCase());
			if(tableConfig.isDistTable()){
				routeToDistTableNode(schema, rrs, ctx.getSql(), tablesAndConditions, cachePool, isSelect);
				return rrs;
			}

			if(retNodesSet.size() > 1 && isAllGlobalTable(ctx, schema)) {
				// mulit routes ,not cache route result
				if (isSelect) {
					rrs.setCacheAble(false);
					ArrayList<String> retNodeList = new ArrayList<String>(retNodesSet);
					Collections.shuffle(retNodeList);//by kaiz : add shuffle
					routeToSingleNode(rrs, retNodeList.get(0), ctx.getSql());
				}
				else {//delete 删除全局表的记录
					routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql(),true);
				}

			} else {
				routeToMultiNode(isSelect, rrs, retNodesSet, ctx.getSql());
			}

		}
		return rrs;

	}


	/**
	 *
	 * 单表路由
	 */
	public static RouteResultset tryRouteForOneTable(SchemaConfig schema, DruidShardingParseInfo ctx,
			RouteCalculateUnit routeUnit, String tableName, RouteResultset rrs, boolean isSelect,
			LayerCachePool cachePool) throws SQLNonTransientException {

		if (isNoSharding(schema, tableName)) {
			return routeToSingleNode(rrs, schema.getDataNode(), ctx.getSql());
		}

		TableConfig tc = schema.getTables().get(tableName);
		if(tc == null) {
			String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}

		if(tc.isDistTable()){
			return routeToDistTableNode(schema,rrs,ctx.getSql(), routeUnit.getTablesAndConditions(), cachePool,isSelect);
		}

		if(tc.isGlobalTable()) {//全局表
			if(isSelect) {
				// global select ,not cache route result
				rrs.setCacheAble(false);
				return routeToSingleNode(rrs, getAliveRandomDataNode(tc)/*getRandomDataNode(tc)*/, ctx.getSql());
			} else {//insert into 全局表的记录
				return routeToMultiNode(false, rrs, tc.getDataNodes(), ctx.getSql(),true);
			}
		} else {//单表或者分库表
			if (!checkRuleRequired(schema, ctx, routeUnit, tc)) {
				throw new IllegalArgumentException("route rule for table "
						+ tc.getName() + " is required: " + ctx.getSql());

			}
			if(tc.getPartitionColumn() == null && !tc.isSecondLevel()) {//单表且不是childTable
//				return RouterUtil.routeToSingleNode(rrs, tc.getDataNodes().get(0),ctx.getSql());
				return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
			} else {
				//每个表对应的路由映射
				Map<String,Set<String>> tablesRouteMap = new HashMap<String,Set<String>>();
				if(routeUnit.getTablesAndConditions() != null && routeUnit.getTablesAndConditions().size() > 0) {
					RouterUtil.findRouteWithcConditionsForTables(schema, rrs, routeUnit.getTablesAndConditions(), tablesRouteMap, ctx.getSql(), cachePool, isSelect);
					if(rrs.isFinishedRoute()) {
						return rrs;
					}
				}

				if(tablesRouteMap.get(tableName) == null) {
					return routeToMultiNode(rrs.isCacheAble(), rrs, tc.getDataNodes(), ctx.getSql());
				} else {
					return routeToMultiNode(rrs.isCacheAble(), rrs, tablesRouteMap.get(tableName), ctx.getSql());
				}
			}
		}
	}

	private static RouteResultset routeToDistTableNode(SchemaConfig schema, RouteResultset rrs,
			String orgSql, Map<String, Map<String, Set<ColumnRoutePair>>> tablesAndConditions,
			LayerCachePool cachePool, boolean isSelect) throws SQLNonTransientException {

		List<String> tables = rrs.getTables();

		String tableName = tables.get(0);
		TableConfig tableConfig = schema.getTables().get(tableName);
		if(tableConfig == null) {
			String msg = "can't find table define in schema " + tableName + " schema:" + schema.getName();
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		if(tableConfig.isGlobalTable()){
			String msg = "can't suport district table  " + tableName + " schema:" + schema.getName() + " for global table ";
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		String partionCol = tableConfig.getPartitionColumn();
		//    String primaryKey = tableConfig.getPrimaryKey();
		boolean isLoadData=false;

		Set<String> tablesRouteSet = new HashSet<String>();

		List<String> dataNodes = tableConfig.getDataNodes();
		if(dataNodes.size()>1){
			String msg = "can't suport district table  " + tableName + " schema:" + schema.getName() + " for mutiple dataNode " + dataNodes;
			LOGGER.warn(msg);
			throw new SQLNonTransientException(msg);
		}
		String dataNode = dataNodes.get(0);

		RouteResultsetNode[] nodes = null;
		//主键查找缓存暂时不实现
		if(tablesAndConditions.isEmpty()){
			List<String> subTables = tableConfig.getDistTables();
			tablesRouteSet.addAll(subTables);

			nodes = getNode(rrs, orgSql, tablesRouteSet, dataNode, false, tableName);
		} else {

			for(Map.Entry<String, Map<String, Set<ColumnRoutePair>>> entry : tablesAndConditions.entrySet()) {
				boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
				Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();

				Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
				if(partitionValue == null || partitionValue.size() == 0) {
					tablesRouteSet.addAll(tableConfig.getDistTables());
				} else {
					for(ColumnRoutePair pair : partitionValue) {
						AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
						if(pair.colValue != null) {
							Integer tableIndex = algorithm.calculate(pair.colValue);
							if(tableIndex == null) {
								String msg = "can't find any valid datanode :" + tableConfig.getName()
										+ " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
								LOGGER.warn(msg);
								throw new SQLNonTransientException(msg);
							}
							String subTable = tableConfig.getDistTables().get(tableIndex);
							if(subTable != null) {
								tablesRouteSet.add(subTable);
								if(algorithm instanceof SlotFunction){
									rrs.getDataNodeSlotMap().put(subTable,((SlotFunction) algorithm).slotValue());
								}
							}
						}
						if(pair.rangeValue != null) {
							Integer[] tableIndexs = algorithm
									.calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
							for(Integer idx : tableIndexs) {
								String subTable = tableConfig.getDistTables().get(idx);
								if(subTable != null) {
									tablesRouteSet.add(subTable);
									if(algorithm instanceof SlotFunction){
										rrs.getDataNodeSlotMap().put(subTable,((SlotFunction) algorithm).slotValue());
									}
								}
							}
						}
					}
				}
			}

			nodes = getNode(rrs, orgSql, tablesRouteSet, dataNode, true, tableName);
		}

		rrs.setNodes(nodes);
		rrs.setSubTables(tablesRouteSet);
		rrs.setFinishedRoute(true);

		return rrs;
	}

	private static RouteResultsetNode[] getNode(RouteResultset rrs, String orgSql, Set<String> tablesRouteSet,
			String dataNode, boolean is, String tableName) {
		Object[] subTables =  tablesRouteSet.toArray();
		RouteResultsetNode[] nodes = new RouteResultsetNode[subTables.length];
		Map<String,Integer> dataNodeSlotMap= rrs.getDataNodeSlotMap();
		for(int i=0;i<nodes.length;i++){
			String table = String.valueOf(subTables[i]);
			String changeSql = orgSql;
			nodes[i] = new RouteResultsetNode(dataNode, rrs.getSqlType(), changeSql);//rrs.getStatement()
			nodes[i].setSubTableName(table);

			if (is) {
				Map<String, List<String>> subTableMaps = rrs.getSubTableMaps();
				if(subTableMaps != null) {
					List<String> list = subTableMaps.get(tableName);
					int index = 0;
					for (String subTable : list) {
						if (table.equals(subTable)) {
							break;
						}
						index++;
					}
					for (String tableSource : subTableMaps.keySet()) {
						Map<String, String> subTableNames = nodes[i].getSubTableNames();
						if (subTableNames == null) {
							subTableNames = Maps.newHashMap();
							nodes[i].setSubTableNames(subTableNames);
						}
						if (tableSource.equals(tableName)) {
							subTableNames.put(tableSource, table);
						} else {
							subTableNames.put(tableSource, subTableMaps.get(tableSource).get(index));
						}

					}
				}
			} else {
				Map<String, List<String>> subTableMaps = rrs.getSubTableMaps();
				if(subTableMaps != null) {
					for (String tableSource : subTableMaps.keySet()) {
						Map<String, String> subTableNames = nodes[i].getSubTableNames();
						if (subTableNames == null) {
							subTableNames = Maps.newHashMap();
							nodes[i].setSubTableNames(subTableNames);
						}
						subTableNames.put(tableSource, subTableMaps.get(tableSource).get(i));
					}
				}
			}

			nodes[i].setSource(rrs);
			if(rrs.getDataNodeSlotMap().containsKey(dataNode)){
				nodes[i].setSlot(rrs.getDataNodeSlotMap().get(dataNode));
			}
			if (rrs.getCanRunInReadDB() != null) {
				nodes[i].setCanRunInReadDB(rrs.getCanRunInReadDB());
			}
			if(dataNodeSlotMap.containsKey(table))  {
				nodes[i].setSlot(dataNodeSlotMap.get(table));
			}
			if(rrs.getRunOnSlave() != null){
				nodes[0].setRunOnSlave(rrs.getRunOnSlave());
			}
		}
		return nodes;
	}

	/**
	 * 处理分库表路由
	 */
	public static void findRouteWithcConditionsForTables(SchemaConfig schema, RouteResultset rrs,
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
			if(tableConfig.getDistTables()!=null && tableConfig.getDistTables().size()>0){
				routeToDistTableNode(schema, rrs, sql, tablesAndConditions, cachePool,isSelect);
			}
			//全局表或者不分库的表略过（全局表后面再计算）
			if(tableConfig.isGlobalTable() || schema.getTables().get(tableName).getDataNodes().size() == 1) {
				continue;
			} else {//非全局表：分库表、childTable、其他
				Map<String, Set<ColumnRoutePair>> columnsMap = entry.getValue();
				String joinKey = tableConfig.getJoinKey();
				String partionCol = tableConfig.getPartitionColumn();
				String primaryKey = tableConfig.getPrimaryKey();
				boolean isFoundPartitionValue = partionCol != null && entry.getValue().get(partionCol) != null;
				boolean isLoadData=false;
				if (LOGGER.isDebugEnabled()
						&& sql.startsWith(LoadData.loadDataHint)||rrs.isLoadData()) {
					//由于load data一次会计算很多路由数据，如果输出此日志会极大降低load data的性能
					isLoadData=true;
				}
				if(entry.getValue().get(primaryKey) != null && entry.getValue().size() == 1&&!isLoadData)
				{//主键查找
					// try by primary key if found in cache
					Set<ColumnRoutePair> primaryKeyPairs = entry.getValue().get(primaryKey);
					if (primaryKeyPairs != null) {
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("try to find cache by primary key ");
						}
						String tableKey = schema.getName() + '_' + tableName;
						boolean allFound = true;
						for (ColumnRoutePair pair : primaryKeyPairs) {//可能id in(1,2,3)多主键
							String cacheKey = pair.colValue;
							String dataNode = (String) cachePool.get(tableKey, cacheKey);
							if (dataNode == null) {
								allFound = false;
								continue;
							} else {
								if(tablesRouteMap.get(tableName) == null) {
									tablesRouteMap.put(tableName, new HashSet<String>());
								}
								tablesRouteMap.get(tableName).add(dataNode);
								continue;
							}
						}
						if (!allFound) {
							// need cache primary key ->datanode relation
							if (isSelect && tableConfig.getPrimaryKey() != null) {
								rrs.setPrimaryKey(tableKey + '.' + tableConfig.getPrimaryKey());
							}
						} else {//主键缓存中找到了就执行循环的下一轮
							continue;
						}
					}
				}
				if (isFoundPartitionValue) {//分库表
					Set<ColumnRoutePair> partitionValue = columnsMap.get(partionCol);
					if(partitionValue == null || partitionValue.size() == 0) {
						if(tablesRouteMap.get(tableName) == null) {
							tablesRouteMap.put(tableName, new HashSet<String>());
						}
						tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
					} else {
						for(ColumnRoutePair pair : partitionValue) {
							AbstractPartitionAlgorithm algorithm = tableConfig.getRule().getRuleAlgorithm();
							if(pair.colValue != null) {
								Integer nodeIndex = algorithm.calculate(pair.colValue);
								if(nodeIndex == null) {
									String msg = "can't find any valid datanode :" + tableConfig.getName()
											+ " -> " + tableConfig.getPartitionColumn() + " -> " + pair.colValue;
									LOGGER.warn(msg);
									throw new SQLNonTransientException(msg);
								}

								ArrayList<String> dataNodes = tableConfig.getDataNodes();
								String node;
								if (nodeIndex >=0 && nodeIndex < dataNodes.size()) {
									node = dataNodes.get(nodeIndex);

								} else {
									node = null;
									String msg = "Can't find a valid data node for specified node index :"
											+ tableConfig.getName() + " -> " + tableConfig.getPartitionColumn()
											+ " -> " + pair.colValue + " -> " + "Index : " + nodeIndex;
									LOGGER.warn(msg);
									throw new SQLNonTransientException(msg);
								}
								if(node != null) {
									if(tablesRouteMap.get(tableName) == null) {
										tablesRouteMap.put(tableName, new HashSet<String>());
									}
									if(algorithm instanceof SlotFunction){
										rrs.getDataNodeSlotMap().put(node,((SlotFunction) algorithm).slotValue());
									}
									tablesRouteMap.get(tableName).add(node);
								}
							}
							if(pair.rangeValue != null) {
								Integer[] nodeIndexs = algorithm
										.calculateRange(pair.rangeValue.beginValue.toString(), pair.rangeValue.endValue.toString());
								ArrayList<String> dataNodes = tableConfig.getDataNodes();
								String node;
								for(Integer idx : nodeIndexs) {
									if (idx >= 0 && idx < dataNodes.size()) {
										node = dataNodes.get(idx);
									} else {
										String msg = "Can't find valid data node(s) for some of specified node indexes :"
												+ tableConfig.getName() + " -> " + tableConfig.getPartitionColumn();
										LOGGER.warn(msg);
										throw new SQLNonTransientException(msg);
									}
									if(node != null) {
										if(tablesRouteMap.get(tableName) == null) {
											tablesRouteMap.put(tableName, new HashSet<String>());
										}
										if(algorithm instanceof SlotFunction){
											rrs.getDataNodeSlotMap().put(node,((SlotFunction) algorithm).slotValue());
										}
										tablesRouteMap.get(tableName).add(node);

									}
								}
							}
						}
					}
				} else if(joinKey != null && columnsMap.get(joinKey) != null && columnsMap.get(joinKey).size() != 0) {//childTable  (如果是select 语句的父子表join)之前要找到root table,将childTable移除,只留下root table
					Set<ColumnRoutePair> joinKeyValue = columnsMap.get(joinKey);

					Set<String> dataNodeSet = ruleByJoinValueCalculate(rrs, tableConfig, joinKeyValue);

					if (dataNodeSet.isEmpty()) {
						throw new SQLNonTransientException(
								"parent key can't find any valid datanode ");
					}
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("found partion nodes (using parent partion rule directly) for child table to update  "
								+ Arrays.toString(dataNodeSet.toArray()) + " sql :" + sql);
					}
					if (dataNodeSet.size() > 1) {
						routeToMultiNode(rrs.isCacheAble(), rrs, dataNodeSet, sql);
						rrs.setFinishedRoute(true);
						return;
					} else {
						rrs.setCacheAble(true);
						routeToSingleNode(rrs, dataNodeSet.iterator().next(), sql);
						return;
					}

				} else {
					//没找到拆分字段，该表的所有节点都路由
					if(tablesRouteMap.get(tableName) == null) {
						tablesRouteMap.put(tableName, new HashSet<String>());
					}
					boolean isSlotFunction= tableConfig.getRule() != null && tableConfig.getRule().getRuleAlgorithm() instanceof SlotFunction;
					if(isSlotFunction){
						for (String dn : tableConfig.getDataNodes()) {
							rrs.getDataNodeSlotMap().put(dn,-1);
						}
					}
					tablesRouteMap.get(tableName).addAll(tableConfig.getDataNodes());
				}
			}
		}
	}

	public static boolean isAllGlobalTable(DruidShardingParseInfo ctx, SchemaConfig schema) {
		boolean isAllGlobal = false;
		for(String table : ctx.getTables()) {
			TableConfig tableConfig = schema.getTables().get(table);
			if(tableConfig!=null && tableConfig.isGlobalTable()) {
				isAllGlobal = true;
			} else {
				return false;
			}
		}
		return isAllGlobal;
	}

	/**
	 *
	 * @param schema
	 * @param ctx
	 * @param tc
	 * @return true表示校验通过，false表示检验不通过
	 */
	public static boolean checkRuleRequired(SchemaConfig schema, DruidShardingParseInfo ctx, RouteCalculateUnit routeUnit, TableConfig tc) {
		if(!tc.isRuleRequired()) {
			return true;
		}
		boolean hasRequiredValue = false;
		String tableName = tc.getName();
		if(routeUnit.getTablesAndConditions().get(tableName) == null || routeUnit.getTablesAndConditions().get(tableName).size() == 0) {
			hasRequiredValue = false;
		} else {
			for(Map.Entry<String, Set<ColumnRoutePair>> condition : routeUnit.getTablesAndConditions().get(tableName).entrySet()) {

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
	 * 增加判断支持未配置分片的表走默认的dataNode
	 * @param schemaConfig
	 * @param tableName
	 * @return
	 */
	public static boolean isNoSharding(SchemaConfig schemaConfig, String tableName) {
		// Table名字被转化为大写的，存储在schema
		tableName = tableName.toUpperCase();
		if (schemaConfig.isNoSharding()) {
			return true;
		}

		if (schemaConfig.getDataNode() != null && !schemaConfig.getTables().containsKey(tableName)) {
			return true;
		}

		return false;
	}

	/**
	 * 系统表判断,某些sql语句会查询系统表或者跟系统表关联
	 * @author lian
	 * @date 2016年12月2日
	 * @param tableName
	 * @return
	 */
	public static boolean isSystemSchema(String tableName) {
		// 以information_schema， mysql开头的是系统表
		if (tableName.startsWith("INFORMATION_SCHEMA.")
				|| tableName.startsWith("MYSQL.")
				|| tableName.startsWith("PERFORMANCE_SCHEMA.")) {
			return true;
		}

		return false;
	}

	/**
	 * 判断条件是否永真
	 * @param expr
	 * @return
	 */
	public static boolean isConditionAlwaysTrue(SQLExpr expr) {
		Object o = WallVisitorUtils.getValue(expr);
		if(Boolean.TRUE.equals(o)) {
			return true;
		}
		return false;
	}

	/**
	 * 判断条件是否永假的
	 * @param expr
	 * @return
	 */
	public static boolean isConditionAlwaysFalse(SQLExpr expr) {
		Object o = WallVisitorUtils.getValue(expr);
		if(Boolean.FALSE.equals(o)) {
			return true;
		}
		return false;
	}


	/**
	 * 该方法，返回是否是ER子表
	 * @param schema
	 * @param origSQL
	 * @param sc
	 * @return
	 * @throws SQLNonTransientException
	 *
	 * 备注说明：
	 *     edit by ding.w at 2017.4.28, 主要处理 CLIENT_MULTI_STATEMENTS(insert into ; insert into)的情况
	 *     目前仅支持mysql,并COM_QUERY请求包中的所有insert语句要么全部是er表，要么全部不是
	 *
	 *
	 */
	public static boolean processERChildTable(final SchemaConfig schema, final String origSQL,
			final ServerConnection sc) throws SQLNonTransientException {

		MySqlStatementParser parser = new MySqlStatementParser(origSQL);
		List<SQLStatement> statements = parser.parseStatementList();

		if(statements == null || statements.isEmpty() ) {
			throw new SQLNonTransientException(String.format("无效的SQL语句:%s", origSQL));
		}


		boolean erFlag = false; //是否是er表
		for(SQLStatement stmt : statements ) {
			MySqlInsertStatement insertStmt = (MySqlInsertStatement) stmt;
			String tableName = insertStmt.getTableName().getSimpleName().toUpperCase();
			final TableConfig tc = schema.getTables().get(tableName);

			if (null != tc && tc.isChildTable()) {
				erFlag = true;

				String sql = insertStmt.toString();

				final RouteResultset rrs = new RouteResultset(sql, ServerParse.INSERT);
				String joinKey = tc.getJoinKey();
				//因为是Insert语句，用MySqlInsertStatement进行parse
//				MySqlInsertStatement insertStmt = (MySqlInsertStatement) (new MySqlStatementParser(origSQL)).parseInsert();
				//判断条件完整性，取得解析后语句列中的joinkey列的index
				int joinKeyIndex = getJoinKeyIndex(insertStmt.getColumns(), joinKey);
				if (joinKeyIndex == -1) {
					String inf = "joinKey not provided :" + tc.getJoinKey() + "," + insertStmt;
					LOGGER.warn(inf);
					throw new SQLNonTransientException(inf);
				}
				//子表不支持批量插入
				if (isMultiInsert(insertStmt)) {
					String msg = "ChildTable multi insert not provided";
					LOGGER.warn(msg);
					throw new SQLNonTransientException(msg);
				}
				//取得joinkey的值
				String joinKeyVal = insertStmt.getValues().getValues().get(joinKeyIndex).toString();
				//解决bug #938，当关联字段的值为char类型时，去掉前后"'"
				String realVal = joinKeyVal;
				if (joinKeyVal.startsWith("'") && joinKeyVal.endsWith("'") && joinKeyVal.length() > 2) {
					realVal = joinKeyVal.substring(1, joinKeyVal.length() - 1);
				}



				// try to route by ER parent partion key
				//如果是二级子表（父表不再有父表）,并且分片字段正好是joinkey字段，调用routeByERParentKey
				RouteResultset theRrs = RouterUtil.routeByERParentKey(sc, schema, ServerParse.INSERT, sql, rrs, tc, realVal);
				if (theRrs != null) {
					boolean processedInsert=false;
					//判断是否需要全局序列号
					if ( sc!=null && tc.isAutoIncrement()) {
						String primaryKey = tc.getPrimaryKey();
						processedInsert=processInsert(sc,schema,ServerParse.INSERT,sql,tc.getName(),primaryKey);
					}
					if(processedInsert==false){
						rrs.setFinishedRoute(true);
						sc.getSession2().execute(rrs, ServerParse.INSERT);
					}
					// return true;
					//继续处理下一条
					continue;
				}

				// route by sql query root parent's datanode
				//如果不是二级子表或者分片字段不是joinKey字段结果为空，则启动异步线程去后台分片查询出datanode
				//只要查询出上一级表的parentkey字段的对应值在哪个分片即可
				final String findRootTBSql = tc.getLocateRTableKeySql().toLowerCase() + joinKeyVal;
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("find root parent's node sql " + findRootTBSql);
				}

				ListenableFuture<String> listenableFuture = MycatServer.getInstance().
						getListeningExecutorService().submit(new Callable<String>() {
					@Override
					public String call() throws Exception {
						FetchStoreNodeOfChildTableHandler fetchHandler = new FetchStoreNodeOfChildTableHandler();
//						return fetchHandler.execute(schema.getName(), findRootTBSql, tc.getRootParent().getDataNodes());
						return fetchHandler.execute(schema.getName(), findRootTBSql, tc.getRootParent().getDataNodes(), sc);
					}
				});


				Futures.addCallback(listenableFuture, new FutureCallback<String>() {
					@Override
					public void onSuccess(String result) {
						//结果为空，证明上一级表中不存在那条记录，失败
						if (Strings.isNullOrEmpty(result)) {
							StringBuilder s = new StringBuilder();
							LOGGER.warn(s.append(sc.getSession2()).append(origSQL).toString() +
									" err:" + "can't find (root) parent sharding node for sql:" + origSQL);
							if(!sc.isAutocommit()) { // 处于事务下失败, 必须回滚
								sc.setTxInterrupt("can't find (root) parent sharding node for sql:" + origSQL);
							}
							sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "can't find (root) parent sharding node for sql:" + origSQL);
							return;
						}

						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("found partion node for child table to insert " + result + " sql :" + origSQL);
						}
						//找到分片，进行插入（和其他的一样，需要判断是否需要全局自增ID）
						boolean processedInsert=false;
						if ( sc!=null && tc.isAutoIncrement()) {
							try {
								String primaryKey = tc.getPrimaryKey();
								processedInsert=processInsert(sc,schema,ServerParse.INSERT,origSQL,tc.getName(),primaryKey);
							} catch (SQLNonTransientException e) {
								LOGGER.warn("sequence processInsert error,",e);
								sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR , "sequence processInsert error," + e.getMessage());
							}
						}
						if(processedInsert==false){
							RouteResultset executeRrs = RouterUtil.routeToSingleNode(rrs, result, origSQL);
							sc.getSession2().execute(executeRrs, ServerParse.INSERT);
						}

					}

					@Override
					public void onFailure(Throwable t) {
						StringBuilder s = new StringBuilder();
						LOGGER.warn(s.append(sc.getSession2()).append(origSQL).toString() +
								" err:" + t.getMessage());
						sc.writeErrMessage(ErrorCode.ER_PARSE_ERROR, t.getMessage() + " " + s.toString());
					}
				}, MycatServer.getInstance().
						getListeningExecutorService());

			} else if(erFlag) {
				throw new SQLNonTransientException(String.format("%s包含不是ER分片的表", origSQL));
			}
		}


		return erFlag;
	}

	/**
	 * 寻找joinKey的索引
	 *
	 * @param columns
	 * @param joinKey
	 * @return -1表示没找到，>=0表示找到了
	 */
	private static int getJoinKeyIndex(List<SQLExpr> columns, String joinKey) {
		for (int i = 0; i < columns.size(); i++) {
			String col = StringUtil.removeBackquote(columns.get(i).toString()).toUpperCase();
			if (col.equals(joinKey)) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * 是否为批量插入：insert into ...values (),()...或 insert into ...select.....
	 *
	 * @param insertStmt
	 * @return
	 */
	private static boolean isMultiInsert(MySqlInsertStatement insertStmt) {
		return (insertStmt.getValuesList() != null && insertStmt.getValuesList().size() > 1)
				|| insertStmt.getQuery() != null;
	}

}
