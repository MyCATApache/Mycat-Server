package io.mycat.server.interceptor.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.fastjson.JSON;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.heartbeat.MySQLConsistencyChecker;
import io.mycat.backend.mysql.nio.MySQLDataSource;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.util.StringUtil;

/**
 * @author digdeep@126.com
 * 全局表一致性检查 和 拦截
 */
public class GlobalTableUtil{
	private static final Logger LOGGER = LoggerFactory.getLogger(GlobalTableUtil.class);
	private static Map<String, TableConfig> globalTableMap = new ConcurrentHashMap<>();
	/** 全局表 保存修改时间戳 的字段名，用于全局表一致性检查 */
	public static final String GLOBAL_TABLE_MYCAT_COLUMN = "_mycat_op_time";
	public static final String COUNT_COLUMN = "record_count";
	public static final String MAX_COLUMN = "max_timestamp";   
	public static final String INNER_COLUMN = "inner_col_exist";   
	private static String operationTimestamp = String.valueOf(new Date().getTime());
	private static volatile int isInnerColumnCheckFinished = 0;
	private static volatile int isColumnCountCheckFinished = 0;
	private static final ReentrantLock lock = new ReentrantLock(false);
	private static List<SQLQueryResult<Map<String, String>>> innerColumnNotExist = new ArrayList<>();
	private static Map<String, String> tableColumsMap = new ConcurrentHashMap<>();
	
	public static Map<String, TableConfig> getGlobalTableMap() {
		return globalTableMap;
	}

	static {
		getGlobalTable();	// 初始化 globalTableMap
	}
	
	public static String interceptSQL(String sql, int sqlType){
		return GlobalTableUtil.consistencyInterceptor(sql, sqlType);
	}
	
	public static String consistencyInterceptor(String sql, int sqlType){
		// 统一使用mycat-server所在机器的时间，防止不同mysqld时间不同步
		operationTimestamp = String.valueOf(new Date().getTime());
		
		LOGGER.debug("before intercept: " +  sql);
		
		if(sqlType == ServerParse.INSERT){
			sql =  convertInsertSQL(sql);
		}
		if(sqlType == ServerParse.UPDATE){
			sql = convertUpdateSQL(sql);
		}
		if(sqlType == ServerParse.DDL){
			LOGGER.info(" DDL to modify global table.");
			sql = handleDDLSQL(sql);	
		}
		
		LOGGER.debug("after intercept: " +  sql);
		/*
		   目前  mycat-server不支持 replace 语句，报错如下：
		 ERROR 1064 (HY000):  ReplaceStatement can't be supported,
		 use insert into ...on duplicate key update... instead
		 
		if(sqlType == ServerParse.REPLACE){
			return convertReplaceSQL(sql);
		}
		*/
		return sql;
	}
	
	/*
	 * Name: 'ALTER TABLE'
		Description:
		Syntax:
		ALTER [IGNORE] TABLE tbl_name
		    [alter_specification [, alter_specification] ...]
		    [partition_options]
	       如果 DDL 修改了表结构，需要重新获得表的列list
	 */
	private static String handleDDLSQL(String sql){
		MySqlStatementParser parser = new MySqlStatementParser(sql);	 
		SQLStatement statement = parser.parseStatement();
		// druid高版本去掉了 MySqlAlterTableStatement，在其父类 SQLAlterTableStatement 直接支持 mysql alter table 语句
//			MySqlAlterTableStatement alter = (MySqlAlterTableStatement)statement;
		SQLExprTableSource source = getDDLTableSource(statement);
		if (source == null)
			return sql;
		String tableName = StringUtil.removeBackquote(source.toString());
		if(StringUtils.isNotBlank(tableName))
			tableName = tableName.trim();
		else
			return sql;
		
		if(!isGlobalTable(tableName))
			return sql;
		
		//增加对全局表create语句的解析，如果是建表语句创建的是全局表，且表中不含"_mycat_op_time"列
		//则为其增加"_mycat_op_time"列，方便导入数据。
		sql = addColumnIfCreate(sql, statement);
		
		final String tn = tableName;
		MycatServer.getInstance().getListeningExecutorService().execute(new Runnable() {
			public void run() {
				try {
					TimeUnit.SECONDS.sleep(3);	// DDL发出之后，等待3秒让DDL分发完成
				} catch (InterruptedException e) {
				} 
				reGetColumnsForTable(tn); // DDL 语句可能会增删 列，所以需要重新获取 全局表的 列list
			}
		});
		
		MycatServer.getInstance().getListeningExecutorService().execute(new Runnable() {
			public void run() {
				try {
					TimeUnit.MINUTES.sleep(10);	// DDL发出之后，等待10分钟再次执行，全局表一般很小，DDL耗时不会超过10分钟
				} catch (InterruptedException e) {
				} 
				reGetColumnsForTable(tn); // DDL 语句可能会增删 列，所以需要重新获取 全局表的 列list
			}
		});
		return sql;
	}

	static String addColumnIfCreate(String sql, SQLStatement statement) {
		if (isCreate(statement) && sql.trim().toUpperCase().startsWith("CREATE TABLE ") && !hasGlobalColumn(statement)) {
			SQLColumnDefinition column = new SQLColumnDefinition();
			column.setDataType(new SQLCharacterDataType("bigint"));
			column.setName(new SQLIdentifierExpr(GLOBAL_TABLE_MYCAT_COLUMN));
			column.setComment(new SQLCharExpr("全局表保存修改时间戳的字段名"));
			((SQLCreateTableStatement)statement).getTableElementList().add(column);
		}
		return statement.toString();
	}
	
	private static boolean hasGlobalColumn(SQLStatement statement){
		for (SQLTableElement tableElement : ((SQLCreateTableStatement)statement).getTableElementList()) {
			SQLName sqlName = null;
			if (tableElement instanceof SQLColumnDefinition) {
				sqlName = ((SQLColumnDefinition)tableElement).getName();
			}
			if (sqlName != null) {
				String simpleName = sqlName.getSimpleName();
				simpleName = StringUtil.removeBackquote(simpleName);
				if (tableElement instanceof SQLColumnDefinition && GLOBAL_TABLE_MYCAT_COLUMN.equalsIgnoreCase(simpleName)) {
					return true;
				}
			}
		}
		return false;
	}

	private static SQLExprTableSource getDDLTableSource(SQLStatement statement) {
		SQLExprTableSource source = null;
		if (statement instanceof SQLAlterTableStatement) {
			source = ((SQLAlterTableStatement)statement).getTableSource();
			
		} else if (isCreate(statement)) {
			source = ((SQLCreateTableStatement)statement).getTableSource();
		}
		return source;
	}

	private static boolean isCreate(SQLStatement statement) {
		return statement instanceof SQLCreateTableStatement;
	}
	
	/**
	 * Syntax:
		INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
	    [INTO] tbl_name
	    [PARTITION (partition_name,...)]
	    [(col_name,...)]
	    {VALUES | VALUE} ({expr | DEFAULT},...),(...),...
	    [ ON DUPLICATE KEY UPDATE
	      col_name=expr
	        [, col_name=expr] ... ]
	
		Or:
	
		INSERT [LOW_PRIORITY | DELAYED | HIGH_PRIORITY] [IGNORE]
	    [INTO] tbl_name
	    [PARTITION (partition_name,...)]
	    SET col_name={expr | DEFAULT}, ...
	    [ ON DUPLICATE KEY UPDATE
	      col_name=expr
	        [, col_name=expr] ... ]
	
		Or:
	
		INSERT [LOW_PRIORITY | HIGH_PRIORITY] [IGNORE]
	    [INTO] tbl_name
	    [PARTITION (partition_name,...)]
	    [(col_name,...)]
	    SELECT ...
	    [ ON DUPLICATE KEY UPDATE
	      col_name=expr
        [, col_name=expr] ... ]
        mysql> insert user value (33333333,'ddd');
		mysql> insert into user value (333333,'ddd');
		mysql> insert user values (3333,'ddd');
     * insert into user(id,name) valueS(1111,'dig'),
     * (1111,  'dig'), (1111,'dig') ,(1111,'dig');
	 * @param sql
	 * @return
	 */
	private static String convertInsertSQL(String sql){
		try{
			MySqlStatementParser parser = new MySqlStatementParser(sql);	 
			SQLStatement statement = parser.parseStatement();
			MySqlInsertStatement insert = (MySqlInsertStatement)statement; 
	        String tableName = StringUtil.removeBackquote(insert.getTableName().getSimpleName());
	        if(!isGlobalTable(tableName))
				return sql;
	        if(!isInnerColExist(tableName))
	        	return sql;
	        	
	        if(insert.getQuery() != null)	// insert into tab select 
	        	return sql;
	        
	        StringBuilder sb = new StringBuilder(200)	// 指定初始容量可以提高性能
			.append("insert into ").append(tableName);
	        
	        List<SQLExpr> columns = insert.getColumns();
	        
	        int idx = -1;	
	        int colSize = -1;
	        
	        if(columns == null || columns.size() <= 0){ // insert 没有带列名：insert into t values(xxx,xxx)
	        	String columnsList = tableColumsMap.get(tableName.toUpperCase());
	        	if(StringUtils.isNotBlank(columnsList)){ //"id,name,_mycat_op_time"
	        		//newSQL = "insert into t(id,name,_mycat_op_time)";
	        		// 构建一个虚拟newSQL来寻找 内部列的索引位置
	        		String newSQL = "insert into " + tableName + "(" + columnsList + ")";
	        		MySqlStatementParser newParser = new MySqlStatementParser(newSQL);	 
	        		SQLStatement newStatement = newParser.parseStatement();
	        		MySqlInsertStatement newInsert = (MySqlInsertStatement)newStatement; 
	        		List<SQLExpr> newColumns = newInsert.getColumns();
	        		for(int i = 0; i < newColumns.size(); i++) {
						String column = StringUtil.removeBackquote(newInsert.getColumns().get(i).toString());
						if(column.equalsIgnoreCase(GLOBAL_TABLE_MYCAT_COLUMN))
							idx = i;	// 找到 内部列的索引位置
					}
	        		colSize = newColumns.size();
	        		sb.append("(").append(columnsList).append(")");
	        	}else{	// tableName 是全局表，但是 tableColumsMap 没有其对应的列list，这种情况不应该存在
		        	LOGGER.warn("you'd better do not use 'insert into t values(a,b)' Syntax (without column list) on global table, "
    				+ "If you do. Then you must make sure inner column '_mycat_op_time' is last column of global table: " 
    				+ tableName + " in all database. Good luck. ^_^");
		        	// 我们假定 内部列位于表中所有列的最后，后面我们在values 子句的最后 给他附加上时间戳
	        	}
	        }else{	// insert 语句带有 列名
	        	sb.append("(");
				for(int i = 0; i < columns.size(); i++) {
					if(i < columns.size() - 1)
						sb.append(columns.get(i).toString()).append(",");
					else
						sb.append(columns.get(i).toString());
					String column = StringUtil.removeBackquote(insert.getColumns().get(i).toString());
					if(column.equalsIgnoreCase(GLOBAL_TABLE_MYCAT_COLUMN))
						idx = i;
				}
				if(idx <= -1)
					sb.append(",").append(GLOBAL_TABLE_MYCAT_COLUMN);
				sb.append(")");
				colSize = columns.size();
	        }
			
			sb.append(" values");
			List<ValuesClause> vcl = insert.getValuesList();
			if(vcl != null && vcl.size() > 1){	// 批量insert
				for(int j=0; j<vcl.size(); j++){
				   if(j != vcl.size() - 1)
					   appendValues(vcl.get(j).getValues(), sb, idx, colSize).append(",");
				   else
					   appendValues(vcl.get(j).getValues(), sb, idx, colSize);
				}
			}else{	// 非批量 insert
				List<SQLExpr> valuse = insert.getValues().getValues();
				appendValues(valuse, sb, idx, colSize);
			}
			
			List<SQLExpr> dku = insert.getDuplicateKeyUpdate();
			if(dku != null && dku.size() > 0){
				sb.append(" on duplicate key update ");
				for(int i=0; i<dku.size(); i++){
					SQLExpr exp = dku.get(i);
					if(exp != null){
						if(i < dku.size() - 1)
							sb.append(exp.toString()).append(",");
						else
							sb.append(exp.toString());
					}
				}
			}
			
			return sb.toString();
		}catch(Exception e){ // 发生异常，则返回原始 sql
			LOGGER.warn(e.getMessage());
			return sql;
		}
	}
	
	public static void main(String[] args){
//		String newSQL = "insert into t(id,name,_mycat_op_time)";// + columnsList + ")";
//		MySqlStatementParser parser = new MySqlStatementParser(newSQL);	 
//		SQLStatement statement = parser.parseStatement();
//		MySqlInsertStatement insert = (MySqlInsertStatement)statement; 
//		List<SQLExpr> columns = insert.getColumns();
//		System.out.println(columns.size());
		
		String sql = "alter table t add colomn name varchar(30)";
		System.out.println(handleDDLSQL(sql));
	}
	
	private static boolean isInnerColExist(String tableName){
		if(innerColumnNotExist.size() > 0){
			for(SQLQueryResult<Map<String, String>> map : innerColumnNotExist){
				if(map != null && tableName.equalsIgnoreCase(map.getTableName())){
					StringBuilder warnStr = new StringBuilder(map.getDataNode())
							.append(".").append(tableName).append(" inner column: ")
							.append(GLOBAL_TABLE_MYCAT_COLUMN)
							.append(" is not exist.");
					LOGGER.warn(warnStr.toString());
					return false;	// tableName 全局表没有内部列
				}
			}
		}
		return true;	// tableName 有内部列
	}
	
	private static StringBuilder appendValues(List<SQLExpr> valuse, StringBuilder sb, int idx, int colSize){
		int size = valuse.size();
		if(size < colSize)
			size = colSize;
		
		sb.append("(");
		for(int i = 0; i < size; i++) {
    		if(i < size - 1){
    			if(i != idx)
    				sb.append(valuse.get(i).toString()).append(",");
    			else
    				sb.append(operationTimestamp).append(",");
    		}else{
    			if(i != idx){
    				sb.append(valuse.get(i).toString());
    			}else{
    				sb.append(operationTimestamp);
    			}
    		}
		}
		if(idx <= -1)
    	   sb.append(",").append(operationTimestamp);
		return sb.append(")");
	}
	
	/**
	 * UPDATE [LOW_PRIORITY] [IGNORE] table_reference
    	SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
    	[WHERE where_condition]
    	[ORDER BY ...]
    	[LIMIT row_count]

		Multiple-table syntax:

		UPDATE [LOW_PRIORITY] [IGNORE] table_references
    	SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
    	[WHERE where_condition]
    	
    	update user, tuser set user.name='dddd',tuser.pwd='aaa' 
    	where user.id=2 and tuser.id=0;
	 * @param sql update tuser set pwd='aaa', name='digdee' where id=0;
	 * @return
	 */
	public static String convertUpdateSQL(String sql){
		try{
			MySqlStatementParser parser = new MySqlStatementParser(sql);	 
			SQLStatement stmt = parser.parseStatement();
			MySqlUpdateStatement update = (MySqlUpdateStatement)stmt;
			SQLTableSource ts = update.getTableSource();
			if(ts != null && ts.toString().contains(",")){
				System.out.println(ts.toString());
				LOGGER.warn("Do not support Multiple-table udpate syntax...");
				return sql;
			}
			
			String tableName = StringUtil.removeBackquote(update.getTableName().getSimpleName());
	        if(!isGlobalTable(tableName))
				return sql;
	        if(!isInnerColExist(tableName))
	        	return sql;		// 没有内部列
	        
			StringBuilder sb = new StringBuilder(150);
			
			SQLExpr se = update.getWhere();
			// where中有子查询： update company set name='com' where id in (select id from xxx where ...)
			if(se instanceof SQLInSubQueryExpr){
				// return sql;
				int idx = sql.toUpperCase().indexOf(" SET ") + 5;
				sb.append(sql.substring(0, idx)).append(GLOBAL_TABLE_MYCAT_COLUMN)
				.append("=").append(operationTimestamp)
				.append(",").append(sql.substring(idx));
				return sb.toString();
			}
			String where = null;
			if(update.getWhere() != null)
				where = update.getWhere().toString();
			
			SQLOrderBy orderBy = update.getOrderBy();
			Limit limit = update.getLimit();
			
			sb.append("update ").append(tableName).append(" set ");
			List<SQLUpdateSetItem> items = update.getItems();
			boolean flag = false;
			for(int i=0; i<items.size(); i++){
				SQLUpdateSetItem item = items.get(i);
				String col = item.getColumn().toString();
				String val = item.getValue().toString();
				
				if(StringUtil.removeBackquote(col)
						.equalsIgnoreCase(GLOBAL_TABLE_MYCAT_COLUMN)){
					flag = true;
					sb.append(col).append("=");
					if(i != items.size() - 1)
						sb.append(operationTimestamp).append(",");
					else
						sb.append(operationTimestamp);
				}else{
					sb.append(col).append("=");
					if(i != items.size() -1 )
						sb.append(val).append(",");
					else
						sb.append(val);
				}
			}
			
			if(!flag){
				sb.append(",").append(GLOBAL_TABLE_MYCAT_COLUMN)
				.append("=").append(operationTimestamp);
			}
			
			sb.append(" where ").append(where);
			
			if(orderBy != null && orderBy.getItems()!=null 
								&& orderBy.getItems().size() > 0){
				sb.append(" order by ");
				for(int i=0; i<orderBy.getItems().size(); i++){
					SQLSelectOrderByItem item = orderBy.getItems().get(i);
					SQLOrderingSpecification os = item.getType();
					sb.append(item.getExpr().toString());
					if(i < orderBy.getItems().size() - 1){
						if(os != null)
							sb.append(" ").append(os.toString());
						sb.append(",");
					}else{
						if(os != null)
							sb.append(" ").append(os.toString());
					}
				}
			}
				
			if(limit != null){		// 分为两种情况： limit 10;   limit 10,10;
				sb.append(" limit ");
				if(limit.getOffset() != null)
					sb.append(limit.getOffset().toString()).append(",");
				sb.append(limit.getRowCount().toString());
			}
			
			return sb.toString();
		}catch(Exception e){
			LOGGER.warn(e.getMessage());
			return sql;
		}
	}
	
	private static void getGlobalTable(){
		MycatConfig config = MycatServer.getInstance().getConfig();
		Map<String, SchemaConfig> schemaMap = config.getSchemas();
		SchemaConfig schemaMconfig = null;
		for(String key : schemaMap.keySet()){
			if(schemaMap.get(key) != null){
				schemaMconfig = schemaMap.get(key);
				Map<String, TableConfig> tableMap = schemaMconfig.getTables();
				if(tableMap != null){
					for(String k : tableMap.keySet()){
						TableConfig table = tableMap.get(k);
						if(table != null && table.isGlobalTable()){
							globalTableMap.put(table.getName().toUpperCase(), table);
						}
					}
				}
			}
		}
	}
	
	/**
	 * 重新获得table 的列list
	 * @param tableName
	 */
	private static void reGetColumnsForTable(String tableName){
		MycatConfig config = MycatServer.getInstance().getConfig();
		if(globalTableMap != null 
						&& globalTableMap.get(tableName.toUpperCase()) != null){
			
			TableConfig tableConfig = globalTableMap.get(tableName.toUpperCase());
			if(tableConfig == null || isInnerColumnCheckFinished != 1)	// consistencyCheck 在运行中
				return;
			
			String nodeName = tableConfig.getDataNodes().get(0);
			
			Map<String, PhysicalDBNode> map = config.getDataNodes();
			for(String k2 : map.keySet()){
				PhysicalDBNode dBnode = map.get(k2);
				if(nodeName.equals(dBnode.getName())){
					PhysicalDBPool pool = dBnode.getDbPool();
					List<PhysicalDatasource> dsList = (List<PhysicalDatasource>)pool.genAllDataSources();
					for(PhysicalDatasource ds : dsList){
						if(ds instanceof MySQLDataSource){
							MySQLDataSource mds = (MySQLDataSource)dsList.get(0);
							MySQLConsistencyChecker checker = 
									new MySQLConsistencyChecker(mds, tableConfig.getName());
							checker.checkInnerColumnExist();
							return; // 运行一次就行了，不需要像consistencyCheck那样每个db都运行一次
						}
					}
				}
			}
		}
	}
	
	public static void consistencyCheck() {
		MycatConfig config = MycatServer.getInstance().getConfig();
		for(String key : globalTableMap.keySet()){
			TableConfig table = globalTableMap.get(key);
			// <table name="travelrecord" dataNode="dn1,dn2,dn3"
			List<String> dataNodeList = table.getDataNodes();
			
			// 记录本次已经执行的datanode
			// 多个 datanode 对应到同一个 PhysicalDatasource 只执行一次
			Map<String, String> executedMap = new HashMap<>();
			for(String nodeName : dataNodeList){	
				Map<String, PhysicalDBNode> map = config.getDataNodes();
				for(String k2 : map.keySet()){
					// <dataNode name="dn1" dataHost="localhost1" database="db1" />
					PhysicalDBNode dBnode = map.get(k2);
					if(nodeName.equals(dBnode.getName())){	// dn1,dn2,dn3
						PhysicalDBPool pool = dBnode.getDbPool();
						Collection<PhysicalDatasource> allDS = pool.genAllDataSources();
						for(PhysicalDatasource pds : allDS){
							if(pds instanceof MySQLDataSource){
								MySQLDataSource mds = (MySQLDataSource)pds;
								if(executedMap.get(pds.getName()) == null){
									MySQLConsistencyChecker checker = 
											new MySQLConsistencyChecker(mds, table.getName());
									
									isInnerColumnCheckFinished = 0;
									checker.checkInnerColumnExist();
									while(isInnerColumnCheckFinished <= 0){
										LOGGER.debug("isInnerColumnCheckFinished:" + isInnerColumnCheckFinished);
										try {
											TimeUnit.SECONDS.sleep(1);
										} catch (InterruptedException e) {
											LOGGER.warn(e.getMessage());
										}
									}
									LOGGER.debug("isInnerColumnCheckFinished:" + isInnerColumnCheckFinished);
									
									// 一种 check 完成之后，再进行另一种 check
									checker = new MySQLConsistencyChecker(mds, table.getName());
									isColumnCountCheckFinished = 0;
									checker.checkRecordCout();
									while(isColumnCountCheckFinished <= 0){
										LOGGER.debug("isColumnCountCheckFinished:" + isColumnCountCheckFinished);
										try {
											TimeUnit.SECONDS.sleep(1);
										} catch (InterruptedException e) {
											LOGGER.warn(e.getMessage());
										}
									}
									LOGGER.debug("isColumnCountCheckFinished:" + isColumnCountCheckFinished);
									
									
									checker = new MySQLConsistencyChecker(mds, table.getName());
									checker.checkMaxTimeStamp();
									
									executedMap.put(pds.getName(), nodeName);
								}
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * 每次处理 一种 check 的结果，不会交叉同时处理 多种不同 check 的结果
	 * @param list
	 * @return
	 */
	public static List<SQLQueryResult<Map<String, String>>>
					finished(List<SQLQueryResult<Map<String, String>>> list){
		lock.lock();
		try{
			//[{"dataNode":"db3","result":{"count(*)":"1"},"success":true,"tableName":"COMPANY"}]
			LOGGER.debug("list:::::::::::" + JSON.toJSONString(list));
			for(SQLQueryResult<Map<String, String>> map : list){
				Map<String, String> row = map.getResult();
				if(row != null){
					if(row.containsKey(GlobalTableUtil.MAX_COLUMN)){
						LOGGER.info(map.getDataNode() + "." + map.getTableName() 
								+ "." + GlobalTableUtil.MAX_COLUMN
								+ ": "+ map.getResult().get(GlobalTableUtil.MAX_COLUMN));
					}
					if(row.containsKey(GlobalTableUtil.COUNT_COLUMN)){
						LOGGER.info(map.getDataNode() + "." + map.getTableName() 
								+ "." + GlobalTableUtil.COUNT_COLUMN
								+ ": "+ map.getResult().get(GlobalTableUtil.COUNT_COLUMN));
					}
					if(row.containsKey(GlobalTableUtil.INNER_COLUMN)){
						String columnsList = null;
						try{
							if(StringUtils.isNotBlank(row.get(GlobalTableUtil.INNER_COLUMN)))
								columnsList = row.get(GlobalTableUtil.INNER_COLUMN); // id,name,_mycat_op_time
							LOGGER.debug("columnsList: " + columnsList);
						}catch(Exception e){
							LOGGER.warn(row.get(GlobalTableUtil.INNER_COLUMN) + ", " + e.getMessage());
						}finally{
							if(columnsList == null 
									|| columnsList.indexOf(GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN) == -1){
								LOGGER.warn(map.getDataNode() + "." + map.getTableName() 
										+ " inner column: " 
										+ GlobalTableUtil.GLOBAL_TABLE_MYCAT_COLUMN
										+ " is not exist.");
								if(StringUtils.isNotBlank(map.getTableName())){
									for(SQLQueryResult<Map<String, String>> sqr : innerColumnNotExist){
										String name = map.getTableName();
										String node = map.getDataNode();
										if(name != null && !name.equalsIgnoreCase(sqr.getTableName())
												|| node != null && !node.equalsIgnoreCase(sqr.getDataNode())){
											innerColumnNotExist.add(map);
										}
									}
								}
							}else{
								LOGGER.debug("columnsList: " + columnsList);
								// COMPANY -> "id,name,_mycat_op_time"，获得了全局表的所有列，并且知道了全局表是否有内部列
								// 所有列，在 insert into t values(xx,yy) 语法中需要用到
								tableColumsMap.put(map.getTableName().toUpperCase(), columnsList);
							}
//							isInnerColumnCheckFinished = 1;
						}
					}
				}
			}
		}finally{
			isInnerColumnCheckFinished = 1;
			isColumnCountCheckFinished = 1;
			lock.unlock();
		}
		return list;
	}
	
	private static boolean isGlobalTable(String tableName){
		if(globalTableMap != null && globalTableMap.size() > 0){
			return globalTableMap.get(tableName.toUpperCase()) != null;
		}
		return false;
	}

	public static Map<String, String> getTableColumsMap() {
		return tableColumsMap;
	}
	
	
}
