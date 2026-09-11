/*
 * Copyright (c) 2020, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.listener.DefaultSqlExecuteStageListener;
import io.mycat.backend.mysql.listener.SqlExecuteStageListener;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.route.RouteResultset;
import io.mycat.server.handler.MysqlProcHandler;
import io.mycat.server.handler.ServerPrepareHandler;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.response.Heartbeat;
import io.mycat.server.response.InformationSchemaProfiling;
import io.mycat.server.response.InformationSchemaProfilingSqlyog;
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
	private long authTimeout = SystemConfig.DEFAULT_AUTH_TIMEOUT;

	/** 保存SET SQL_SELECT_LIMIT的值, default 解析为-1. */
	private volatile  int sqlSelectLimit = -1;
	private volatile  boolean txReadonly;
	private volatile int txIsolation;
	private volatile boolean autocommit;
	private volatile boolean preAcStates; //上一个ac状态,默认为true
	private volatile boolean txInterrupted;
	private volatile String txInterrputMsg = "";
	private long lastInsertId;
	private NonBlockingSession session;
	/**
	 * 标志是否执行了lock tables语句，并处于lock状态
	 */
	private volatile boolean isLocked = false;
    private Queue<SqlEntry> executeSqlQueue;
    private SqlExecuteStageListener listener;
    // 记录本连接创建的所有预处理语句ID
    private final Set<Long> preparedStatementIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
	
	public ServerConnection(NetworkChannel channel)
			throws IOException {
		super(channel);
		this.txInterrupted = false;
		this.autocommit = true;
		this.preAcStates = true;
		this.txReadonly = false;
        this.executeSqlQueue = new LinkedBlockingQueue<>();
        this.listener = new DefaultSqlExecuteStageListener(this);
	}

	/**
	 * 添加预处理语句ID到连接跟踪
	 * @param pstmtId 预处理语句ID
	 */
	public void addPreparedStatementId(long pstmtId) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Tracking preparestatement: {} for connection: {}", pstmtId, id);
		}
		preparedStatementIds.add(pstmtId);
	}

	/**
	 * 从连接跟踪中移除预处理语句ID
	 * @param pstmtId 预处理语句ID
	 */
	public void removePreparedStatementId(long pstmtId) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Removing preparestatement tracking: {} for connection: {}", pstmtId, id);
		}
		preparedStatementIds.remove(pstmtId);
	}

	/**
	 * 清理所有预处理语句资源
	 */
	public void clearPreparedStatements() {
		int statementCount = preparedStatementIds.size();
		if (statementCount > 0) {
			LOGGER.info("Cleaning up {} preparestatements for closing connection: {}", statementCount, id);
			// 遍历并输出每个要清理的pstmtId
			for (Long pstmtId : preparedStatementIds) {
				// 在关闭前先记录pstmtId
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Closing preparestatement with id: {} for connection: {}", pstmtId, id);
				}
				ServerPrepareHandler.closePreparedStatement(pstmtId);
			}
			if (LOGGER.isInfoEnabled() && statementCount > 0) {
				LOGGER.info("Cleaned preparestatement IDs: {}", preparedStatementIds);
			}
		} else {
			LOGGER.debug("No preparestatements to clean for connection: {}", id);
		}
		preparedStatementIds.clear();
	}

	@Override
	public boolean isIdleTimeout() {
		if (isAuthenticated) {
			return super.isIdleTimeout();
		} else {
			return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + this.authTimeout;
		}
	}

	public long getAuthTimeout() {
		return authTimeout;
	}

	public void setAuthTimeout(long authTimeout) {
		this.authTimeout = authTimeout;
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

	public boolean isTxReadonly() {
		return txReadonly;
	}

	public void setTxReadonly(boolean txReadonly) {
		this.txReadonly = txReadonly;
	}

	public int getSqlSelectLimit() {
		return sqlSelectLimit;
	}

	public void setSqlSelectLimit(int sqlSelectLimit) {
		this.sqlSelectLimit = sqlSelectLimit;
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
	
	/**
	 * 
	 * 清空食事务中断
	 * */
	public void clearTxInterrupt() {
		if (!autocommit && txInterrupted) {
			txInterrupted = false;
			this.txInterrputMsg = "";
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
				db = MycatServer.getInstance().getConfig().getUsers().get(user).getDefaultSchema();
				if (db == null) {
					writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
							"No MyCAT Database selected");
					return ;
				}
			}
			isDefault = false;
		}
		
		// 兼容PhpAdmin's, 支持对MySQL元数据的模拟返回
		//// TODO: 2016/5/20 支持更多information_schema特性
//		if (ServerParse.SELECT == type
//				&& db.equalsIgnoreCase("information_schema") ) {
//			MysqlInformationSchemaHandler.handle(sql, this);
//			return;
//		}

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

		//fix sqlyog select state, round(sum(duration),5) as `duration (summed) in sec` from information_schema.profiling where query_id = 0 group by state order by `duration (summed) in sec` desc
		if(ServerParse.SELECT == type &&sql.contains(" information_schema.profiling ")&&sql.contains("duration (summed) in sec"))
		{
			InformationSchemaProfilingSqlyog.response(this);
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
			db = SchemaUtil.detectDefaultDb(sql, type);
			if (db == null){
				db = MycatServer.getInstance().getConfig().getUsers().get(user).getDefaultSchema();
				if (db == null) {
					writeErrMessage(ErrorCode.ERR_BAD_LOGICDB,
							"No MyCAT Database selected");
					return null;
				}
			}

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
			return;
		}
		if (rrs != null) {
            // #支持mariadb驱动useBatchMultiSend=true,连续接收到的sql先放入队列，等待前面处理完成后再继续处理。
            // 参考https://mariadb.com/kb/en/option-batchmultisend-description/
            boolean executeNow = false;
            synchronized (this.executeSqlQueue) {
                executeNow = this.executeSqlQueue.isEmpty();
                this.executeSqlQueue.add(new SqlEntry(sql, type, rrs));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("add queue,executeSqlQueue size {}", executeSqlQueue.size());
                }
            }

            if (executeNow) {
                this.executeSqlId++;
                session.execute(rrs, rrs.isSelectForUpdate() ? ServerParse.UPDATE : type);
            }
        }
    }

	/**
	 * 提交事务
	 */
	public void commit() {
		if (txInterrupted) {
			LOGGER.warn("receive commit ,but found err message in Transaction {}",this);
			this.rollback();
//			writeErrMessage(ErrorCode.ER_YES,
//					"Transaction error, need to rollback.");
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
		LOGGER.info("Closing connection {} due to: {}", id, reason);
		super.close(reason);
		
		// 清理所有预处理资源
		clearPreparedStatements();
		
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
				+ ", autocommit=" + autocommit + ", schema=" + schema+ ", executeSql=" + executeSql + "]" +
				this.getSession2();
		
	}

	public boolean isPreAcStates() {
		return preAcStates;
	}

	public void setPreAcStates(boolean preAcStates) {
		this.preAcStates = preAcStates;
	}

    public SqlExecuteStageListener getListener() {
        return listener;
    }

    public void setListener(SqlExecuteStageListener listener) {
        this.listener = listener;
    }

    @Override
    public void checkQueueFlow() {
        RouteResultset rrs = session.getRrs();
        if (rrs != null && rrs.getNodes().length > 1 && session.getRrs().needMerge()) {
            // 多节点合并结果集语句需要拉取所有数据，无法流控
            return;
        } else {
            // 非合并结果集语句进行流量控制检查。
            flowController.check(session.getTargetMap());
        }
    }

    @Override
    public void resetConnection() {
        // 1 简单点直接关闭后端连接。若按照mysql官方的提交事务或回滚事务，mycat都会回包给应用，引发包乱序。
        session.closeAndClearResources("receive com_reset_connection");

        // 2 重置用户变量
        this.txInterrupted = false;
        this.autocommit = true;
        this.preAcStates = true;
        this.txReadonly = false;
        this.lastInsertId = 0;

        super.resetConnection();
    }
    /**
     * sql执行完成后回调函数
     */
    public void onEventSqlCompleted() {
        SqlEntry sqlEntry = null;
        synchronized (this.executeSqlQueue) {
            this.executeSqlQueue.poll();// 弹出已经执行成功的
            sqlEntry = this.executeSqlQueue.peek();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("poll queue,executeSqlQueue size {}", this.executeSqlQueue.size());
            }
        }
        if (sqlEntry != null) {
            this.executeSqlId++;
            session.execute(sqlEntry.rrs, sqlEntry.rrs.isSelectForUpdate() ? ServerParse.UPDATE : sqlEntry.type);
        }
    }

    private class SqlEntry {
        public String sql;
        public int type;
        public RouteResultset rrs;

        public SqlEntry(String sql, int type, RouteResultset rrs) {
            this.sql = sql;
            this.type = type;
            this.rrs = rrs;
        }
    }

}
