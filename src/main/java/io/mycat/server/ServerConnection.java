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
package io.mycat.server;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.DataType;
import io.mycat.backend.mysql.nio.handler.MiddlerQueryResultHandler;
import io.mycat.backend.mysql.nio.handler.MiddlerResultHandler;
import io.mycat.backend.mysql.nio.handler.MultiNodeQueryHandler;
import io.mycat.backend.mysql.nio.handler.SecondHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.route.RouteResultset;
import io.mycat.server.handler.MysqlInformationSchemaHandler;
import io.mycat.server.handler.MysqlProcHandler;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.response.Heartbeat;
import io.mycat.server.response.InformationSchemaProfiling;
import io.mycat.server.response.Ping;
import io.mycat.server.util.SchemaUtil;
import io.mycat.util.SplitUtil;
import io.mycat.util.TimeUtil;

/**
 * @author mycat
 */
public class ServerConnection extends FrontendConnection {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ServerConnection.class);
	private static final long AUTH_TIMEOUT = 15 * 1000L;

	private volatile int txIsolation;
	private volatile boolean autocommit;
	private volatile boolean txInterrupted;
	private volatile String txInterrputMsg = "";
	private long lastInsertId;
	private NonBlockingSession session;
	/**
	 * 标志是否执行了lock tables语句，并处于lock状态
	 */
	private volatile boolean isLocked = false;

	public ServerConnection(NetworkChannel channel)
			throws IOException {
		super(channel);
		this.txInterrupted = false;
		this.autocommit = true;
	}

	@Override
	public boolean isIdleTimeout() {
		if (isAuthenticated) {
			return super.isIdleTimeout();
		} else {
			return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
					lastReadTime) + AUTH_TIMEOUT;
		}
	}

	public int getTxIsolation() {
		return txIsolation;
	}

	public void setTxIsolation(int txIsolation) {
		this.txIsolation = txIsolation;
	}

	public boolean isAutocommit() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		this.autocommit = autocommit;
	}

	public long getLastInsertId() {
		return lastInsertId;
	}

	public void setLastInsertId(long lastInsertId) {
		this.lastInsertId = lastInsertId;
	}

	/**
	 * 设置是否需要中断当前事务
	 */
	public void setTxInterrupt(String txInterrputMsg) {
		if (!autocommit && !txInterrupted) {
			txInterrupted = true;
			this.txInterrputMsg = txInterrputMsg;
		}
	}

	public boolean isTxInterrupted()
	{
		return txInterrupted;
	}
	public NonBlockingSession getSession2() {
		return session;
	}

	public void setSession2(NonBlockingSession session2) {
		this.session = session2;
	}
	
	public boolean isLocked() {
		return isLocked;
	}

	public void setLocked(boolean isLocked) {
		this.isLocked = isLocked;
	}

	@Override
	public void ping() {
		Ping.response(this);
	}

	@Override
	public void heartbeat(byte[] data) {
		Heartbeat.response(this, data);
	}

	public void execute(String sql, int type) {
		//连接状态检查
		if (this.isClosed()) {
			LOGGER.warn("ignore execute ,server connection is closed " + this);
			return;
		}
		// 事务状态检查
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback." + txInterrputMsg);
			return;
		}

		// 检查当前使用的DB
		String db = this.schema;
		boolean isDefault = true;
		if (db == null) {
			db = SchemaUtil.detectDefaultDb(sql, type);
			if (db == null) {
				writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No MyCAT Database selected");
				return;
			}
			isDefault = false;
		}
		
		// 兼容PhpAdmin's, 支持对MySQL元数据的模拟返回
		//// TODO: 2016/5/20 支持更多information_schema特性
		if (ServerParse.SELECT == type 
				&& db.equalsIgnoreCase("information_schema") ) {
			MysqlInformationSchemaHandler.handle(sql, this);
			return;
		}

		if (ServerParse.SELECT == type 
				&& sql.contains("mysql") 
				&& sql.contains("proc")) {
			
			SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
			if (schemaInfo != null 
					&& "mysql".equalsIgnoreCase(schemaInfo.schema)
					&& "proc".equalsIgnoreCase(schemaInfo.table)) {
				
				// 兼容MySQLWorkbench
				MysqlProcHandler.handle(sql, this);
				return;
			}
		}
		
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(db);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + db + "'");
			return;
		}

		//fix navicat   SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `Percentage` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ
		if(ServerParse.SELECT == type &&sql.contains(" INFORMATION_SCHEMA.PROFILING ")&&sql.contains("CONCAT(ROUND(SUM(DURATION)/"))
		{
			InformationSchemaProfiling.response(this);
			return;
		}
		
		/* 当已经设置默认schema时，可以通过在sql中指定其它schema的方式执行
		 * 相关sql，已经在mysql客户端中验证。
		 * 所以在此处增加关于sql中指定Schema方式的支持。
		 */
		if (isDefault && schema.isCheckSQLSchema() && isNormalSql(type)) {
			SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
			if (schemaInfo != null && schemaInfo.schema != null && !schemaInfo.schema.equals(db)) {
				SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schemaInfo.schema);
				if (schemaConfig != null)
					schema = schemaConfig;
			}
		}

		routeEndExecuteSQL(sql, type, schema);

	}
	
	private boolean isNormalSql(int type) {
		return ServerParse.SELECT==type||ServerParse.INSERT==type||ServerParse.UPDATE==type||ServerParse.DELETE==type||ServerParse.DDL==type;
	}

    public RouteResultset routeSQL(String sql, int type) {

		// 检查当前使用的DB
		String db = this.schema;
		if (db == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"No MyCAT Database selected");
			return null;
		}
		SchemaConfig schema = MycatServer.getInstance().getConfig()
				.getSchemas().get(db);
		if (schema == null) {
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
					"Unknown MyCAT Database '" + db + "'");
			return null;
		}

		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),
							schema, type, sql, this.charset, this);

		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(),e);
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
			return null;
		}
		return rrs;
	}




	public void routeEndExecuteSQL(String sql, final int type, final SchemaConfig schema) {
		sql = sql.toUpperCase();
		String original = sql;
		
 
 		String middleSql = "";

		int rsType = ServerParse.parse(sql);
		int sqlType = rsType & 0xff;
		boolean canClose = true;
		//对双重select进行代理执行
		//step1:判断是一个select语句 
	    if(sqlType == ServerParse.SELECT ){
			   //step2:判断select的个数,只处理2个的情况
	    	String array[] = sql.toUpperCase().split("SELECT ");
	    	if(array !=null && array.length == 3){
				canClose = false;
	    		//step3:得到第一步要处理的select语句
	    		String last = array[2];		
	    		char begin1='(';
	    		char end1 = ')';
	    		//得到第一步需要执行的sql 
	    		for(int i=0;i<last.length();i++){
	    			
	    			char c = last.charAt(i);
	    			if(c == end1){
 	    				String str1 = last.substring(0,i+1);
	    				int a1 = appearNumber(str1, "\\(");
	    				int a2 = appearNumber(str1, "\\)");
	    				if(a1 !=a2){
	    					 middleSql = last.substring(0,i);
	    					 break;
	    				}
	    			}
	    		 
	    		}
	    		
	    		middleSql = "SELECT " + middleSql;
		    	int beginIndex = sql.indexOf(middleSql);
		    	final String beginSql = sql.substring(0, beginIndex);
 		    	int middleIndex = beginIndex + middleSql.length();	
		    	final String endSql = sql.substring(middleIndex,sql.length()); 
		    	original = middleSql;
		    	//执行第二部sql
		    	MiddlerResultHandler	middlerResultHandler = null;
				  middlerResultHandler =  new MiddlerQueryResultHandler<String>(  new SecondHandler() {						 
						@Override
						public void doExecute(String param) {
							session.setMiddlerResultHandler(null);
							String sqls = beginSql +param+endSql;
							// 路由计算
							RouteResultset rrs = null;
							try {
								rrs = MycatServer
										.getInstance()
										.getRouterservice()
										.route(MycatServer.getInstance().getConfig().getSystem(),
												schema, type,sqls.toLowerCase(), charset,ServerConnection.this );

							} catch (Exception e) {
								StringBuilder s = new StringBuilder();
								LOGGER.warn(s.append(this).append(sqls).toString() + " err:" + e.toString(),e);
								String msg = e.getMessage();
								writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
								return;
							}
							NonBlockingSession noBlockSession =  new NonBlockingSession(session.getSource());
							noBlockSession.setMiddlerResultHandler(null);
							//session的预编译标示传递
							noBlockSession.setPrepared(session.isPrepared());
							if (rrs != null) {
								
								MultiNodeQueryHandler multiNodeQueryHandler = 	new MultiNodeQueryHandler(rrs.isSelectForUpdate()?ServerParse.UPDATE:type, rrs, autocommit, session);
								
								noBlockSession.setCanClose(false);
								noBlockSession.execute(rrs, type);
							 
							}
						}
					} );
				  session.setMiddlerResultHandler(middlerResultHandler);
	    	}
	    	
	    	
		}
	
	    //
		if(!canClose){
			session.setCanClose(canClose);
		}
		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),
							schema, type, original.toLowerCase(), this.charset, this);

		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(),e);
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
			return;
		}
		 
		
		if (rrs != null) {
 			// session执行			
			session.execute(rrs, rrs.isSelectForUpdate()?ServerParse.UPDATE:type);
			//session.setMiddlerResultHandler(null);
		}
	    
 	}

	/**
	 * 提交事务
	 */
	public void commit() {
		if (txInterrupted) {
			writeErrMessage(ErrorCode.ER_YES,
					"Transaction error, need to rollback.");
		} else {
			session.commit();
		}
	}

	/**
	 * 回滚事务
	 */
	public void rollback() {
		// 状态检查
		if (txInterrupted) {
			txInterrupted = false;
		}

		// 执行回滚
		session.rollback();
	}
	/**
	 * 执行lock tables语句方法
	 * @param sql
	 */
	public void lockTable(String sql) {
		// 事务中不允许执行lock table语句
		if (!autocommit) {
			writeErrMessage(ErrorCode.ER_YES, "can't lock table in transaction!");
			return;
		}
		// 已经执行了lock table且未执行unlock table之前的连接不能再次执行lock table命令
		if (isLocked) {
			writeErrMessage(ErrorCode.ER_YES, "can't lock multi-table");
			return;
		}
		RouteResultset rrs = routeSQL(sql, ServerParse.LOCK);
		if (rrs != null) {
			session.lockTable(rrs);
		}
	}
	
	/**
	 * 执行unlock tables语句方法
	 * @param sql
	 */
	public void unLockTable(String sql) {
		sql = sql.replaceAll("\n", " ").replaceAll("\t", " ");
		String[] words = SplitUtil.split(sql, ' ', true);
		if (words.length==2 && ("table".equalsIgnoreCase(words[1]) || "tables".equalsIgnoreCase(words[1]))) {
			isLocked = false;
			session.unLockTable(sql);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
		}
		
	}

	/**
	 * 撤销执行中的语句
	 * 
	 * @param sponsor
	 *            发起者为null表示是自己
	 */
	public void cancel(final FrontendConnection sponsor) {
		processor.getExecutor().execute(new Runnable() {
			@Override
			public void run() {
				session.cancel(sponsor);
			}
		});
	}

	@Override
	public void close(String reason) {
		super.close(reason);
		session.terminate();
		if(getLoadDataInfileHandler()!=null)
		{
			getLoadDataInfileHandler().clear();
		}
	}

	/**
	 * add huangyiming 检测字符串中某字符串出现次数
	 * @param srcText
	 * @param findText
	 * @return
	 */
	public static int appearNumber(String srcText, String findText) {
	    int count = 0;
	    Pattern p = Pattern.compile(findText);
	    Matcher m = p.matcher(srcText);
	    while (m.find()) {
	        count++;
	    }
	    return count;
	}
	@Override
	public String toString() {
		return "ServerConnection [id=" + id + ", schema=" + schema + ", host="
				+ host + ", user=" + user + ",txIsolation=" + txIsolation
				+ ", autocommit=" + autocommit + ", schema=" + schema + "]";
	}

}
