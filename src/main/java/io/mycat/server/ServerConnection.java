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
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.handler.MysqlInformationSchemaHandler;
import io.mycat.server.handler.MysqlProcHandler;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.response.Heartbeat;
import io.mycat.server.response.InformationSchemaProfiling;
import io.mycat.server.response.Ping;
import io.mycat.server.util.SchemaUtil;
import io.mycat.sqlengine.AllJobFinishedListener;
import io.mycat.sqlengine.EngineCtx;
import io.mycat.sqlengine.MultiRowSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJobHandler;
import io.mycat.util.ArrayUtil;
import io.mycat.util.SplitUtil;
import io.mycat.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.util.List;
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

	public static final String[] mysqlSelfDbs = {"information_schema","mysql","performance_schema","sys"};
	
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
	
	public boolean isTxInterrupted() {
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
	 * 获取后端数据库连接
	 * @param schema
	 * @return
	 */
	private BackendConnection getBackendConnection(String dataHosts, String schema){
		BackendConnection con = MycatServer.getInstance().getConfig()
				.getDataHosts().get(dataHosts).getSource().getConMap().tryTakeCon(schema, autocommit);
		if(con==null){
			return null;
		}
		return con;
	}

	/**
	 * MySQL自带数据库（information_schema,mysql,performance_schema,sys）的操作
	 * 或是类似SET NAMES utf8mb4;USE `数据库名`;select @@character_set_databased的操作
	 * 需要发到关联的所有MySQL执行并返回结果
	 * @param sql
	 * @param type
	 */
	private void doDBSelfTableOpt(final String sql, final int type){
		if(this.schema!=null){
			SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(this.schema);
			if(schemaConfig!=null){
				try {
					LayerCachePool cachePool = (LayerCachePool) MycatServer.getInstance().getCacheService().getCachePool("TableID2DataNodeCache");
					RouteResultset rrs = RouteStrategyFactory.getRouteStrategy().route(MycatServer.getInstance().getConfig().getSystem(), schemaConfig, type, sql, this.charset,
							this, cachePool);
					session.execute(rrs, rrs.isSelectForUpdate() ? ServerParse.UPDATE:type);
				}catch (Exception e){
					StringBuilder s = new StringBuilder();
					LOGGER.warn(s.append(this).append(sql).toString() + " err:" + e.toString(),e);
					String msg = e.getMessage();
					writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e.getClass().getSimpleName() : msg);
				}
				return;
			}
		}
		MysqlInformationSchemaHandler.handle(sql, this);
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

        // TODO 以下逻辑有问题，需要改为以下方式
        // 1. 应先分析sql
        // 2. 是否包含数据库。如果是走3；如果不是走11
        // 3. 是否有相对应的逻辑库配置。如果是走4；如果不是走7
        // 4. 是否包含数据表。如果是走5；如果不是走10
        // 5. 是否有对应的逻辑表配置。如果是走6；如果不是走9
        // 6. 路由分析并发给相应的MySQL执行并返回结果
        // 7. 是否是MySQL自带数据库（information_schema,mysql,performance_schema,sys）的操作
        //    或是类似SET NAMES utf8mb4;USE `数据库名`;select @@character_set_databased的操作。如果是走8；如果不是走9
        // 8. 发到关联的所有MySQL执行并返回结果
        // 9. 返回提示不支持信息
        // 10. 是否对数据库进行操作。如果是走8；如果不是走9
        // 11. 是否包含数据表。如果是走5；如果不是走11

        // 分析sql
        Map<String, SchemaConfig> schemaConfigMap = MycatServer.getInstance().getConfig().getSchemas();
        SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
        String schema = null;
        String table = null;
        SchemaConfig schemaConfig = null;
        TableConfig tableConfig = null;
        if(schemaInfo!=null){
            if(schemaInfo.schema!=null){
                schema = schemaInfo.schema;
            }
            if(schemaInfo.table!=null){
                table = schemaInfo.table;
            }
        }
        if(schema == null && table == null){
            // 设置操作 如
            // select @@collation_database;
            // SET NAMES utf8mb4;
            // SHOW VARIABLES LIKE 'lower_case_%';
            // SHOW FULL TABLES WHERE Table_Type != 'VIEW';
            // SHOW TABLE STATUS;
            // show table status like '表名';
            // show create table `表名`;
            // SHOW DATABASES;
            // 等
			doDBSelfTableOpt(sql, type);
            return;
        }
        if(schemaConfigMap==null || schemaConfigMap.size()==0){
            // Mycat没有逻辑库配置
            String msg = "Mycat has no configuration information";
            LOGGER.warn(msg);
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, msg);
            return;
        }
        if(schema!=null){
            if(ArrayUtil.arraySearch(mysqlSelfDbs,schema.toLowerCase())){
                // MySQL自带数据库的查询
                if ("mysql".equalsIgnoreCase(schema)
                        && "proc".equalsIgnoreCase(table)) {
                    // 兼容MySQLWorkbench
                    MysqlProcHandler.handle(sql, this);
                    return;
                }else if("information_schema".equalsIgnoreCase(schema)){
                    if(ServerParse.SELECT == type
                            && "profiling".equalsIgnoreCase(table)
                            && sql.toUpperCase().trim().contains("CONCAT(ROUND(SUM(DURATION)/")){
                        //fix navicat
                        // SELECT STATE AS `State`, ROUND(SUM(DURATION),7) AS `Duration`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `Percentage`
                        // FROM INFORMATION_SCHEMA.PROFILING
                        // WHERE QUERY_ID=
                        // GROUP BY STATE
                        // ORDER BY SEQ
                        InformationSchemaProfiling.response(this);
                        return;
                    }
					// fix navicat
					// SELECT action_order, event_object_table, trigger_name, event_manipulation, event_object_table, definer, action_statement, action_timing
					// FROM information_schema.triggers
					// WHERE BINARY event_object_schema = '数据库名' AND BINARY event_object_table = '数据表名'
					// ORDER BY event_object_table
					// SELECT COUNT(*)
					// FROM information_schema.TABLES
					// WHERE TABLE_SCHEMA = '数据库名'
					// UNION SELECT COUNT(*)
					// FROM information_schema.COLUMNS
					// WHERE TABLE_SCHEMA = '数据库名'
					// UNION SELECT COUNT(*)
					// FROM information_schema.ROUTINES
					// WHERE ROUTINE_SCHEMA = '数据库名'
					doDBSelfTableOpt(sql, type);
                    return;
                }else{
                    // 兼容PhpAdmin's, 支持对MySQL元数据的模拟返回
                    MysqlInformationSchemaHandler.handle(sql, this);
                    return;
                }
            } else {
                schemaConfig = schemaConfigMap.get(schema);
                if(schemaConfig==null){
                    // 不在Mycat逻辑库配置里 不支持的数据库
                    String msg = "Mycat does not support this schema:" + schema;
                    LOGGER.warn(msg);
                    writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, msg);
                    return;
                }
            }
        }
        if(table!=null){
            if(schema!=null){
                if(schemaConfig==null){
                    schemaConfig = schemaConfigMap.get(schema);
                }
                if(schemaConfig!=null){
                    tableConfig = schemaConfig.getTables().get(table.toUpperCase());
                }
            }else{
                for(String schemaKey : schemaConfigMap.keySet()){
                    Map<String, TableConfig> tableConfigMap = schemaConfigMap.get(schemaKey).getTables();
                    if(tableConfigMap==null || tableConfigMap.size()==0){
                        continue;
                    }
                    for(String tableKey : tableConfigMap.keySet()){
                        TableConfig itemConfig = tableConfigMap.get(tableKey);
                        if(tableKey.equalsIgnoreCase(table) && itemConfig!=null){
                            schemaConfig = schemaConfigMap.get(schemaKey);
                            schema = schemaKey;
                            tableConfig = itemConfig;
                            break;
                        }
                    }
                    if(tableConfig!=null){
                        break;
                    }
                }
            }
        }
        if(schema == null){
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No MyCAT Database selected");
            return;
        }
        if (schemaConfig == null) {
            String msg = "Unknown MyCAT Database '" + schema + "'";
            LOGGER.warn(msg);
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, msg);
            return;
        }
        if(tableConfig==null){
            // 不在Mycat的逻辑表配置里 不支持的数据表
            String msg = "Mycat does not support this table:" + table;
            LOGGER.warn(msg);
            writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, msg);
            return;
        }
        this.schema = schema;
        routeEndExecuteSQL(sql, type, schemaConfig);
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
				+ ", autocommit=" + autocommit + ", schema=" + schema+ ", executeSql=" + executeSql + "]" +
				this.getSession2();
		
	}

	public boolean isPreAcStates() {
		return preAcStates;
	}

	public void setPreAcStates(boolean preAcStates) {
		this.preAcStates = preAcStates;
	}

}

class DirectDBHandler implements SQLJobHandler {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DirectDBHandler.class);

	private final EngineCtx ctx;
	private final String[] dataNodes;
	private List<byte[]> fields;

	public DirectDBHandler(EngineCtx ctx, String[] dataNodes){
		this.ctx = ctx;
		this.dataNodes = dataNodes;
	}

	@Override
	public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
		this.fields = fields;
	}

	@Override
	public boolean onRowData(String dataNode, byte[] rowData) {
		return false;
	}

	@Override
	public void finished(String dataNode, boolean failed, String errorMsg) {
		if(failed){
			String msg = "dataNode:"+dataNode+" error:"+errorMsg;
			LOGGER.error(msg);
			ctx.getSession().getSource().writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, errorMsg);
		}else{
			ctx.endJobInput();
		}
	}
}