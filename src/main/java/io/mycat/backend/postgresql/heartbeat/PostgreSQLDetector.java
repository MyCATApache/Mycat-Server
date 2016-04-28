package io.mycat.backend.postgresql.heartbeat;

import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.backend.heartbeat.MySQLHeartbeat;
import io.mycat.backend.postgresql.PostgreSQLDataSource;
import io.mycat.config.model.DataHostConfig;
import io.mycat.sqlengine.OneRawSQLQueryResultHandler;
import io.mycat.sqlengine.SQLJob;
import io.mycat.sqlengine.SQLQueryResult;
import io.mycat.sqlengine.SQLQueryResultListener;
import io.mycat.util.TimeUtil;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgreSQLDetector implements
		SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {

	private static final String[] MYSQL_SLAVE_STAUTS_COLMS = new String[] {
			"Seconds_Behind_Master", "Slave_IO_Running", "Slave_SQL_Running" };

	private PostgreSQLHeartbeat heartbeat;

	private final AtomicBoolean isQuit;

	private volatile long heartbeatTimeout;

	private volatile long lastSendQryTime;

	private volatile SQLJob sqlJob;

	private long lasstReveivedQryTime;

	public PostgreSQLDetector(PostgreSQLHeartbeat heartbeat) {
		this.heartbeat = heartbeat;
		this.isQuit = new AtomicBoolean(false);
	}

	@Override
	public void onResult(SQLQueryResult<Map<String, String>> result) {
		if (result.isSuccess()) {
			int balance = heartbeat.getSource().getDbPool().getBalance();
			PhysicalDatasource source = heartbeat.getSource();
			Map<String, String> resultResult = result.getResult();
			if (source.getHostConfig().isShowSlaveSql()
					&& (source.getHostConfig().getSwitchType() == DataHostConfig.SYN_STATUS_SWITCH_DS || PhysicalDBPool.BALANCE_NONE != balance)) {

				String Slave_IO_Running = resultResult != null ? resultResult
						.get("Slave_IO_Running") : null;
				String Slave_SQL_Running = resultResult != null ? resultResult
						.get("Slave_SQL_Running") : null;
				if (Slave_IO_Running != null
						&& Slave_IO_Running.equals(Slave_SQL_Running)
						&& Slave_SQL_Running.equals("Yes")) {
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_NORMAL);
					String Seconds_Behind_Master = resultResult
							.get("Seconds_Behind_Master");
					if (null != Seconds_Behind_Master
							&& !"".equals(Seconds_Behind_Master)) {
						heartbeat.setSlaveBehindMaster(Integer
								.valueOf(Seconds_Behind_Master));
					}
				} else if (source.isSalveOrRead()) {
					MySQLHeartbeat.LOGGER
							.warn("found MySQL master/slave Replication err !!! "
									+ heartbeat.getSource().getConfig());
					heartbeat.setDbSynStatus(DBHeartbeat.DB_SYN_ERROR);
				}

			}
			heartbeat.setResult(PostgreSQLHeartbeat.OK_STATUS, this, null);
		} else {
			heartbeat.setResult(PostgreSQLHeartbeat.ERROR_STATUS, this, null);
		}
		lasstReveivedQryTime = System.currentTimeMillis();
	}

	public PostgreSQLHeartbeat getHeartbeat() {
		return heartbeat;
	}

	public long getHeartbeatTimeout() {
		return heartbeatTimeout;
	}

	public void heartbeat() {
		lastSendQryTime = System.currentTimeMillis();
		PostgreSQLDataSource ds = heartbeat.getSource();
		String databaseName = ds.getDbPool().getSchemas()[0];
		String[] fetchColms = {};
		if (heartbeat.getSource().getHostConfig().isShowSlaveSql()) {
			fetchColms = MYSQL_SLAVE_STAUTS_COLMS;
		}
		OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(
				fetchColms, this);
		sqlJob = new SQLJob(heartbeat.getHeartbeatSQL(), databaseName,
				resultHandler, ds);
		sqlJob.run();
	}

	public void close(String msg) {
		SQLJob curJob = sqlJob;
		if (curJob != null && !curJob.isFinished()) {
			curJob.teminate(msg);
			sqlJob = null;
		}
	}

	public boolean isHeartbeatTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastSendQryTime,
				lasstReveivedQryTime) + heartbeatTimeout;
	}

	public long getLastSendQryTime() {
		return lastSendQryTime;
	}

	public long getLasstReveivedQryTime() {
		return lasstReveivedQryTime;
	}

	public void quit() {
	}

	public boolean isQuit() {
		return isQuit.get();
	}

}
