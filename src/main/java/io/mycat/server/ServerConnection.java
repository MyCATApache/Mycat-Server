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

import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.route.RouteResultset;
import io.mycat.server.handler.MysqlInformationSchemaHandler;
import io.mycat.server.handler.MysqlProcHandler;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.response.Heartbeat;
import io.mycat.server.response.InformationSchemaProfiling;
import io.mycat.server.response.Ping;
import io.mycat.server.util.SchemaUtil;
import io.mycat.util.ArrayUtil;
import io.mycat.util.SplitUtil;
import io.mycat.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 前端服务器连接 SQL请求
 *
 * @author mycat
 */
public class ServerConnection extends FrontendConnection {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ServerConnection.class);
	private static final long AUTH_TIMEOUT = 15 * 1000L;

	// 事务隔离级别
	private volatile int txIsolation;
	// 自动提交标识
	private volatile boolean autocommit;
	//上一个ac状态,默认为true
	private volatile boolean preAcStates;
	// 事务中断标识
	private volatile boolean txInterrupted;
	// 事务中断信息
	private volatile String txInterrputMsg = "";
	// 最后插入id
	private long lastInsertId;
	// 非阻塞Session 中间结果路由使用
	private NonBlockingSession session;
	/**
	 * 标志是否执行了lock tables语句，并处于lock状态
	 */
	private volatile boolean isLocked = false;

    private final String[] mysqlSelfDbs = {"information_schema","mysql","performance_schema","sys"};

    public ServerConnection(NetworkChannel channel)
			throws IOException {
		super(channel);
		this.txInterrupted = false;
		this.autocommit = true;
		this.preAcStates = true;
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

	/**
	 * 执行sql
	 * @param sql
	 * @param type
	 */
	public void execute(String sql, int type) {
        //连接状态检查
        if (this.isClosed()) {
            LOGGER.warn("ignore execute, server connection is closed " + this);
            return;
        }
        // 事务状态检查
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES,
                    "Transaction error, need to rollback." + txInterrputMsg);
            return;
        }

		// TODO 以下逻辑有问题，需要改为以下方式 http://note.youdao.com/noteshare?id=9d96a5d30dbedb26e1ef581b19b42a99
		// 1. 应先分析sql
		// 2. 是否包含数据库。如果是走3；如果不是走11
		// 3. 是否有相对应的逻辑库配置。如果是走4；如果不是走7
		// 4. 是否包含数据表。如果是走5；如果不是走10
		// 5. 是否有对应的逻辑表配置。如果是走6；如果不是走9
		// 6. 路由分析并发给相应的MySQL执行并返回结果
		// 7. 是否是MySQL自带数据库（information_schema,mysql,performance_schema,sys）的操作或是类似SET NAMES utf8mb4;USE `数据库名`;select @@character_set_databased的操作。如果是走8；如果不是走9
		// 8. 发到关联的所有MySQL执行并返回结果
		// 9. 返回提示不支持信息
		// 10. 是否对数据库进行操作。如果是走8；如果不是走9
		// 11. 是否包含数据表。如果是走5；如果不是走7

		// 检查当前使用的DB
		String db = this.schema;
		boolean isDefault = true;
		if (db == null) {
			// 检测默认逻辑库 数据库
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
			// 解析逻辑库 数据库
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
			writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "Unknown MyCAT Database '" + db + "'");
			return;
		}

		//fix navicat   SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `Percentage` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ
		if(ServerParse.SELECT == type
				&& sql.toUpperCase().contains(" INFORMATION_SCHEMA.PROFILING ")
				&& sql.toUpperCase().trim().contains("CONCAT(ROUND(SUM(DURATION)/")) {
			InformationSchemaProfiling.response(this);
			return;
		}

		/**
		 * 当已经设置默认schema时，可以通过在sql中指定其它schema的方式执行
		 * 相关sql，已经在mysql客户端中验证。
		 * 所以在此处增加关于sql中指定Schema方式的支持。
		 */
		if (isDefault
				&& schema.isCheckSQLSchema()
				&& isNormalSql(type)) {
			SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
			if (schemaInfo != null
					&& schemaInfo.schema != null
					&& !schemaInfo.schema.equals(db)) {
				// 获取另一个逻辑库 数据库配置
				SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(schemaInfo.schema);
				if (schemaConfig != null) {
					schema = schemaConfig;
				}
			}
		}
		routeEndExecuteSQL(sql, type, schema);
	}

	/**
	 * 是否是普通SQL
	 * @param type
	 * @return
	 */
	private boolean isNormalSql(int type) {
		return ServerParse.SELECT==type
				|| ServerParse.INSERT==type
				|| ServerParse.UPDATE==type
				|| ServerParse.DELETE==type
				|| ServerParse.DDL==type;
	}

	/**
	 * 路由
	 * @param sql
	 * @param type
	 * @return
	 */
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


	/**
	 * 路由结束并执行SQL
	 * @param sql
	 * @param type
	 * @param schema
	 */
	public void routeEndExecuteSQL(String sql, final int type, final SchemaConfig schema) {
		// 路由计算
		RouteResultset rrs = null;
		try {
			rrs = MycatServer
					.getInstance()
					.getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(), schema, type, sql, this.charset, this);

		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(),e);
			String msg = e.getMessage();
			writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
			return;
		}
		if (rrs != null) {
			// session执行
			session.execute(rrs, rrs.isSelectForUpdate() ? ServerParse.UPDATE:type);
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
		if(getLoadDataInfileHandler()!=null) {
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

	public boolean isPreAcStates() {
		return preAcStates;
	}

	public void setPreAcStates(boolean preAcStates) {
		this.preAcStates = preAcStates;
	}

}
