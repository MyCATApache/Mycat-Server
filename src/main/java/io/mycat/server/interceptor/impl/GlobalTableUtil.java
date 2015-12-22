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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import io.mycat.MycatServer;
import io.mycat.backend.MySQLDataSource;
import io.mycat.backend.PhysicalDBNode;
import io.mycat.backend.PhysicalDBPool;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.backend.heartbeat.MySQLConsistencyChecker;
import io.mycat.server.config.node.MycatConfig;
import io.mycat.server.config.node.SchemaConfig;
import io.mycat.server.config.node.TableConfig;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.SQLQueryResult;

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
	private static final ReentrantLock lock = new ReentrantLock(false);
	private static List<SQLQueryResult<Map<String, String>>> innerColumnNotExist = new ArrayList<>();
	
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
		String tableName = getTableName(sql, sqlType);
		if(StringUtils.isBlank(tableName))
			return sql;
		if(!isGlobalTable(tableName))
			return sql;
		
		LOGGER.debug("sqlType: " + sqlType);
		LOGGER.debug("before sql: " +  sql);
		
		if(innerColumnNotExist.size() > 0){
			for(SQLQueryResult<Map<String, String>> map : innerColumnNotExist){
				if(map != null && tableName.equalsIgnoreCase(map.getTableName())){
					StringBuilder warnStr = new StringBuilder(map.getDataNode())
							.append(".").append(tableName).append(" inner column: ")
							.append(GLOBAL_TABLE_MYCAT_COLUMN)
							.append(" is not exist.");
					LOGGER.warn(warnStr.toString());
					return sql;
				}
			}
		}
		
		if(sqlType == ServerParse.INSERT){
			sql =  convertInsertSQL(sql, tableName);
		}
		if(sqlType == ServerParse.UPDATE){
			sql = convertUpdateSQL(sql, tableName);
		}
		/*
		   目前  mycat-server不支持 replace 语句，报错如下：
		 ERROR 1064 (HY000):  ReplaceStatement can't be supported,
		 use insert into ...on duplicate key update... instead
		 
		if(sqlType == ServerParse.REPLACE){
			return convertReplaceSQL(sql);
		}
		*/
		LOGGER.debug("after sql: " +  sql);
		return sql;
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
	
		Or: mycat-server 不支持该语法
	
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
	private static String convertInsertSQL(String sql, String tableName){
		// insert into tb select
		if(Pattern.matches(".*(?i)insert\\s+(into\\s+)?.*select.*", sql)){
			return sql;
		}
		try{
			if(!isGlobalTable(tableName))
				return sql;
			
			// 处理  insert into set 语句
			// insert into company set name=xx, _mycat_op_time=11111111,addr=xxx; 
			if(Pattern.matches(".*(?i)insert\\s+(into\\s+)?.*\\s+set\\s+.*", sql)){
         		String regStr = ".*(?i)insert.*(into\\s+)?set.*" + GLOBAL_TABLE_MYCAT_COLUMN + "\\s*=.*";
         		return dealWithUpdateOrInsertSet(regStr, sql, "insert");
			}
			
			// 获取列名： id,name,addr
			String re = ".*(?i)insert\\s+(into)?\\s+.*\\((.*)\\)\\s+value(s)?.*";
			Matcher matcher = Pattern.compile(re).matcher(sql);
			String cols = null;
			if(matcher.find())
				cols = matcher.group(2);
			if((cols != null && cols.indexOf(')') != -1)
					|| tableName.indexOf(')') != -1)	//排除特殊情况： 如果表名列名中含有 ')'
				return sql;
				
			int index = sql.indexOf(')');
			String insertStr = sql.substring(0, index);	// insert into user(id,name,_mycat_op_time
			String valuesStr = sql.substring(index);	// ) values(1111,'dig',11111);
			
			// 批量插入： values 后至少有两个 ()()
			String multiValuesReg = ".*(?i)insert.*(into\\s+)?.*value.*\\(.*\\).*\\(.*\\).*";
			if(Pattern.matches(multiValuesReg, sql)){
				String reg = ".*(?i)insert.*"+GLOBAL_TABLE_MYCAT_COLUMN+".*value.*\\(.*\\).*\\(.*\\).*";
				// values 后至少有两个 ()()，并且带有 GLOBAL_TABLE_MYCAT_COLUMN 列
				if(Pattern.matches(reg, sql)){	
					LOGGER.warn("Do not insert value to inner col: "+GLOBAL_TABLE_MYCAT_COLUMN);
					return replaceMultiValue(sql);
				}
				// values 后至少有两个 ()()，没有 GLOBAL_TABLE_MYCAT_COLUMN 列
				// insert into user(id,name) valueS(1111,'dig'),(1111,'dig');
				// valuesStr = ") valueS(1111,'dig'),(1111,'dig');"
				int colNum = insertStr.split("\\s+,\\s+").length;
				String[] strArr = valuesStr.split("\\)\\s*,\\s*"); // 将 values以 '),'分割
				StringBuilder newSQL = new StringBuilder(insertStr).append(",")
											.append(GLOBAL_TABLE_MYCAT_COLUMN);
				for(int i=0; i< strArr.length; i++){
					if(colNum != strArr[i].split("\\s+,\\s+").length)
						return sql;
					if(i == strArr.length-1){
						int idx = strArr[i].indexOf(")");
						newSQL.append(strArr[i].substring(0, idx));
						newSQL.append(",").append(operationTimestamp)
						.append(strArr[i].substring(idx));
					}else{
						newSQL.append(strArr[i]).append(",").
						append(operationTimestamp).append("),");
					}
				}
				return newSQL.toString();
			}
			
			// 非批量插入
			
			String oneValuesReg = ".*(?i)insert.*(into\\s+)?.*value.*\\(.*\\).*";
			if(Pattern.matches(oneValuesReg, sql)){
				String reg = ".*(?i)insert.*" + GLOBAL_TABLE_MYCAT_COLUMN + ".*value.*\\(.*\\).*";
				// insert into company(name,_mycat_op_time,addr) values(xx,11111111,xxx);
				if(Pattern.matches(reg, sql)){
					LOGGER.warn("Do not insert value to inner col: " + GLOBAL_TABLE_MYCAT_COLUMN);
					return replaceColValue(sql);
				}
				
				// sql = "insert into company(name,addr) values(xx,xxx);"
				// insertStr = "insert into company(name,addr"
				// valuesStr = ") values(xx,xxx);"
				int appendIndex = valuesStr.lastIndexOf(')');
				String valueStr1 = valuesStr.substring(0, appendIndex);	// ) values(xx,xxx
				String valueStr2 = valuesStr.substring(appendIndex);	// );
				String newSQL = new StringBuilder(insertStr)
					.append(",").append(GLOBAL_TABLE_MYCAT_COLUMN)
					.append(valueStr1).append(",")
					.append(operationTimestamp).append(valueStr2).toString();
				return newSQL;
			}
		}catch(Exception e){ // 发生异常，则返回原始 sql
			LOGGER.warn(e.getMessage());
			return sql;
		}
		return sql;
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
	private static String convertUpdateSQL(String sql, String tableName){
		// update user, tuser set user.name='dddd',tuser.pwd='aaa' 
        if(tableName.indexOf(',') != -1){	
        	 LOGGER.warn("Do not support Multiple-table udpate syntax...");
        	 return sql;
        }
        try{
        	if(!isGlobalTable(tableName))
        		return sql;
        	
    		// update company set name=xx, _mycat_op_time=11111111,sex=xxx where id=2
     		String regStr = ".*(?i)update.*set.*" + GLOBAL_TABLE_MYCAT_COLUMN + "\\s*=.*";
     		dealWithUpdateOrInsertSet(regStr, sql, "update");
     		
        }catch(Exception e){
        	LOGGER.warn(e.getMessage());
			return sql;
        }
        return sql;
	}
	
	private static String dealWithUpdateOrInsertSet(String regStr, String sql, String type){
		// update      company set name=xx, _mycat_op_time=11111111,addr=xxx where id=2
		// insert into company set name=xx, _mycat_op_time=11111111,addr=xxx;
 		if(Pattern.matches(regStr, sql)){
 			if("insert".equalsIgnoreCase(type)){
				LOGGER.warn("Do not insert value to inner column: " + GLOBAL_TABLE_MYCAT_COLUMN);
 			}
 			if("update".equalsIgnoreCase(type)){
 				LOGGER.warn("Do not update value to inner column: " + GLOBAL_TABLE_MYCAT_COLUMN);
 			}
 			// "insert into company set name=xx, _mycat_op_time=11111111,addr=xxx;";
 			// "insert into company set name=xx, _mycat_op_time='11111111',addr=xxx;";
 			// "insert into company set name=xx, _mycat_op_time=\"11111111\",addr=xxx";
 			return sql.replaceAll(GLOBAL_TABLE_MYCAT_COLUMN+"=[\"|']{0,1}\\d*[\"|']{0,1}", 
 							      GLOBAL_TABLE_MYCAT_COLUMN+"="+operationTimestamp);
 		}
		
 		StringBuilder newSQL = new StringBuilder();
 		
 		// insert into company set name=xx,addr=xxx; 
 		if("insert".equalsIgnoreCase(type)){
 			sql = sql.trim();
 			if(sql.indexOf(';') != sql.length()-1){	// 尾部没有 ;
				 return newSQL.append(sql)
						.append(",").append(GLOBAL_TABLE_MYCAT_COLUMN)
						.append("=").append(operationTimestamp).toString();
			}else{
				return newSQL.append(sql.substring(0, sql.length()-1))	// 先去掉末尾的;
						.append(",").append(GLOBAL_TABLE_MYCAT_COLUMN).append("=")
						.append(operationTimestamp).append(";").toString();	// 在加上末尾的;
			}
 		}
 		
 		// update company set name=xx,addr=xxx where id=2;
 		if("update".equalsIgnoreCase(type)){
 			int idx = sql.toUpperCase().indexOf(" SET ") + 5;
 			String udpateStr = sql.substring(0, idx);	// update company set 
 			String setStr = sql.substring(idx);			// name=xx,addr=xxx where id=2;
 			return new StringBuilder(udpateStr).append(GLOBAL_TABLE_MYCAT_COLUMN)
 					.append("=").append(operationTimestamp).append(",")
 					.append(setStr).toString();
 		}
 		return sql;
	}
	
	/**
	 * Syntax:
		REPLACE [LOW_PRIORITY | DELAYED]
		    [INTO] tbl_name
		    [PARTITION (partition_name,...)]
		    [(col_name,...)]
		    {VALUES | VALUE} ({expr | DEFAULT},...),(...),...
		
		Or:
		
		REPLACE [LOW_PRIORITY | DELAYED]
		    [INTO] tbl_name
		    [PARTITION (partition_name,...)]
		    SET col_name={expr | DEFAULT}, ...
		
		Or:
		
		REPLACE [LOW_PRIORITY | DELAYED]
		    [INTO] tbl_name
		    [PARTITION (partition_name,...)]
		    [(col_name,...)]
		    SELECT ...
		    
		mysql> replace into user(id,name) values(2,'aaa');
		mysql> replace user(id,name) values(2,'aaa');
		mysql> replace user(id,name) value(2,'aaa');
		mysql> replace into user(id,name) value(2,'aaa');
		mysql> replace user(id,name) values(2,'aaa'),(3,'bbb');
		mysql> replace tuser set name='digdeep', pwd='aaaa';
		replace into user set name='aaa'  (不能带where)
		replace user set name='aaa' 	  (不能带where)
		 等价于不带 where 的update:
		update user set name='aaa'
		
		目前 replace mycat-server不支持，报错如下：
		ERROR 1064 (HY000):  ReplaceStatement can't be supported,
		use insert into ...on duplicate key update... instead
	 * @param sql
	 * @return
	 */
	private static String convertReplaceSQL(String sql){
		// mycat-server 不支持该语法: replace into tb select;
		if(Pattern.matches(".*(?i)replace.*(into\\s+)?.*select.*", sql)){
			return sql;
		}
		String regStr = ".*(?i)replace.*\\(.*" + GLOBAL_TABLE_MYCAT_COLUMN + ".*";
 		if(Pattern.matches(regStr, sql)){
 			LOGGER.warn("client cannot replace value to inner column: " + GLOBAL_TABLE_MYCAT_COLUMN);
 			return sql;
 		}
		
		String multiValuesReg = ".*(?i)replace.*(into\\s+)?.*value.*\\(.*\\).*\\(.*\\).*"; 
		String replaceSetReg = ".*(?i)replace.*(into\\s+)?.*set.*"; 
		try{
			int index = sql.indexOf(')');
			String insertStr = sql.substring(0, index);
			String valuesStr = sql.substring(index);
			String tableName = insertStr.substring(insertStr.toUpperCase().indexOf("INTO ")+5,
											insertStr.indexOf('('));
			tableName = tableName.trim();
			
			if(isGlobalTable(tableName)){
				// replace set
				if(Pattern.matches(replaceSetReg, sql)){
					StringBuilder newSQL = new StringBuilder();
					if(sql.indexOf(";") == -1){	// 尾部没有 ;
						return newSQL.append(sql).append(",").append(GLOBAL_TABLE_MYCAT_COLUMN)
								.append("=").append(operationTimestamp).toString();
					}else{	// 尾部有 ;
						return newSQL.append(sql.substring(0, sql.indexOf(";"))).append(",")
								.append(GLOBAL_TABLE_MYCAT_COLUMN)
								.append("=").append(operationTimestamp).append(";").toString();
					}
				}
				
				// replace  values
				if(Pattern.matches(multiValuesReg, sql)){
					String[] strArr = valuesStr.split("\\)\\s*,\\s*");	// 将 values 部分 以  '),' 分割
					StringBuilder newSQL = new StringBuilder(insertStr).append(",")
												.append(GLOBAL_TABLE_MYCAT_COLUMN);
					for(int i=0; i< strArr.length; i++){
						if(i == strArr.length-1){
							int idx = strArr[i].indexOf(")");
							newSQL.append(strArr[i].substring(0, idx));
							newSQL.append(",").append(operationTimestamp)
							.append(strArr[i].substring(idx));
						}else
							newSQL.append(strArr[i]).append(",")
							.append(operationTimestamp).append("),");
					}
					return newSQL.toString();
				}else{
					int appendIndex = valuesStr.lastIndexOf(')');
					String valueStr1 = valuesStr.substring(0, appendIndex);
					String valueStr2 = valuesStr.substring(appendIndex);
					
					return new StringBuilder(insertStr).append(",")
							.append(GLOBAL_TABLE_MYCAT_COLUMN)
							.append(valueStr1).append(",")
							.append(operationTimestamp).append(valueStr2).toString();
				}
			}
		}catch(Exception e){
			LOGGER.warn(e.getMessage());
			return sql;
		}
		return sql;
	}
	
	private static String getTableName(String sql, int sqlType){
		String tableName = null;
		if(sqlType == ServerParse.INSERT){
			// 注意必须使用非贪婪模式
			String reg = ".*(?i)insert\\s+(into\\s+)?(.+?)\\(.*"; // into 关键字是可选的
			Matcher matcher = Pattern.compile(reg).matcher(sql);
			if(matcher.find()){
				tableName = matcher.group(2);
	        }
			return  tableName != null ? tableName.trim() : null;
		}
		if(sqlType == ServerParse.UPDATE){
			// 注意必须使用非贪婪模式
			String reg = ".*(?i)update(.+?)set.*"; // 提取表名
			Matcher matcher = Pattern.compile(reg).matcher(sql);
			if(matcher.find()){
				tableName = matcher.group(1);
	        }
	        return  tableName != null ? tableName.trim() : null;
		}
		return null;
	}
	
	/**
	 * insert into company(id,name,addr,_mycat_op_time) 
	 * 	values(7,'b','b',11111111);
	 */
	private static String replaceColValue(String sql){
		try{
			int index = sql.indexOf(')');
			String insertStr = sql.substring(0, index);	// insert into user(id,name,_mycat_op_time
			String valuesStr = sql.substring(index);	// ) values(1111,'dig',11111);
			if(insertStr.split("\\s*,\\s*").length != valuesStr.split("\\s*,\\s*").length)
				return sql;	// 字段的值中包含了字符 ',' 所以分割得到的数组长度不一致
			
			int columnIndex = getColIdxForInsert(insertStr);
			StringBuilder sb = new StringBuilder(insertStr)
							.append(replaceIndexValue(valuesStr, columnIndex));
			return sb.toString();
		}catch(Exception e){
			e.printStackTrace();
			return sql;
		}
	}
	
	/**
	 * 根据内部列  _mycat_op_time 的idx，将对应idx位置的value替换掉 
	 * valuesStr ) values(1111,'dig',11111);
	 * (1111,'dig',2222
	 * (1111,'dig',2222);
	 * ) valueS(1111,'dig',22
	 */
	private static String replaceIndexValue(String valuesStr, int columnIndex){
		String[] values = valuesStr.split("\\s*,\\s*");
		StringBuilder sb = new StringBuilder();
		for(int j=0; j<values.length; j++){
			if(j != columnIndex){
				sb.append(values[j]).append(",");
			}else{
				if(columnIndex == values.length - 1)
					sb.append(operationTimestamp)
					.append(values[columnIndex].substring(values[columnIndex].indexOf(")")));
				else
					sb.append(operationTimestamp).append(",");
			}
		}
		return sb.toString();
	}
	
	private static int getColIdxForInsert(String insertStr){
		String[] columns = insertStr.split("\\s*,\\s*");
		for(int i=0; i<columns.length; i++){
			if(columns[i] != null && columns[i].toLowerCase()
					.contains(GLOBAL_TABLE_MYCAT_COLUMN.toLowerCase())){
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * 根据内部列  _mycat_op_time 的idx，将对于idx位置的value替换掉 
	 * insert into user(id,name) valueS(1111,'dig'),(1111,'dig'),(1111,'dig'),(1111,'dig');
	 * insert into user(id,name,_mycat_op_time) valueS(1111,'dig',22),(1111,'dig',22);
	 * @return
	 */
	private static String replaceMultiValue(String sql){
		int index = sql.indexOf(')');
		String insertStr = sql.substring(0, index);	// insert into user(id,name,_mycat_op_time
		String valuesStr = sql.substring(index);	// ) valueS(1111,'dig',22),(1111,'dig',22);
		int columnNum = insertStr.split("\\s*,\\s*").length;
		int columnIndex = getColIdxForInsert(insertStr);
	
		// ) valueS(1111,'dig',22),(1111,'dig',22),(1111,'dig',22);
		String[] strArr = valuesStr.split("\\)\\s*,\\s*"); //将 values以 '),'分割:  (1111,'dig',22
		
		StringBuilder newSQL = new StringBuilder(insertStr)
						.append(",").append(GLOBAL_TABLE_MYCAT_COLUMN);
			
		for(int i=0; i< strArr.length; i++){
			if(i == strArr.length-1){
				// (1111,'dig',2222);
				if(columnNum != strArr[i].split("\\s*,\\s*").length)
					return sql;
				String str = replaceIndexValue(strArr[i], columnIndex);	
				newSQL.append(str);
			}else{
				// ) valueS(1111,'dig',22
				// (1111,'dig',22
				if(columnNum != strArr[i].split("\\s*,\\s*").length)
					return sql;
				String str = replaceIndexValue(strArr[i], columnIndex);	
				newSQL.append(str);
			}
		}
		
		return newSQL.toString();
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
									
									checker.checkInnerColumnExist();
									while(isInnerColumnCheckFinished <= 0){
										LOGGER.debug("isInnerColumnCheckFinished:" + isInnerColumnCheckFinished);
										try {
											TimeUnit.SECONDS.sleep(1);
										} catch (InterruptedException e) {
											e.printStackTrace();
										}
									}
									LOGGER.debug("isInnerColumnCheckFinished:" + isInnerColumnCheckFinished);
									
									checker = new MySQLConsistencyChecker(mds, table.getName());
									checker.checkRecordCout();
									try {
										TimeUnit.SECONDS.sleep(1);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
									
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
						int count = 0;
						try{
							if(StringUtils.isNotBlank(row.get(GlobalTableUtil.INNER_COLUMN)))
								count = Integer.parseInt(row.get(GlobalTableUtil.INNER_COLUMN).trim());
						}catch(NumberFormatException e){
							LOGGER.warn(row.get(GlobalTableUtil.INNER_COLUMN) + ", " + e.getMessage());
						}finally{
							if(count <= 0){
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
							}
							isInnerColumnCheckFinished = 1;
						}
					}
				}
			}
		}finally{
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
}
