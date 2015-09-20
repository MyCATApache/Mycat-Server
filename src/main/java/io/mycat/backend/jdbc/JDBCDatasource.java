package io.mycat.backend.jdbc;

import io.mycat.backend.PhysicalDatasource;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.net.ConnectIdGenerator;
import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.executors.ResponseHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.google.common.collect.Lists;

public class JDBCDatasource extends PhysicalDatasource {
	static {
		// 加载可能的驱动
		List<String> drivers = Lists.newArrayList("com.mysql.jdbc.Driver",
				"org.opencloudb.jdbc.mongodb.MongoDriver",
				"org.opencloudb.jdbc.sequoiadb.SequoiaDriver",
				"oracle.jdbc.OracleDriver",
				"com.microsoft.sqlserver.jdbc.SQLServerDriver",
				"org.apache.hive.jdbc.HiveDriver", "com.ibm.db2.jcc.DB2Driver",
				"org.postgresql.Driver");
		for (String driver : drivers) {
			try {
				Class.forName(driver);
			} catch (ClassNotFoundException ignored) {
			}
		}
	}

	public JDBCDatasource(DBHostConfig config, DataHostConfig hostConfig,
			boolean isReadNode) {
		super(config, hostConfig, isReadNode);

	}

	@Override
	public DBHeartbeat createHeartBeat() {
		return new JDBCHeartbeat(this);
	}

	@Override
	public void createNewConnection(ResponseHandler handler, String schema)
			throws IOException {
		DBHostConfig cfg = getConfig();
		JDBCConnection c = new JDBCConnection();

		c.setHost(cfg.getIp());
		c.setPort(cfg.getPort());
		c.setPool(this);
		c.setSchema(schema);
		c.setDbType(cfg.getDbType());
		c.setId(ConnectIdGenerator.getINSTNCE().getId()); // 复用mysql的Backend的ID，需要在process中存储
		try {

			Connection con = getConnection();
			// c.setIdleTimeout(pool.getConfig().getIdleTimeout());
			c.setCon(con);
			// notify handler
			handler.connectionAcquired(c);
		} catch (Exception e) {
			handler.connectionError(e, c);
		}

	}

	Connection getConnection() throws SQLException {
		DBHostConfig cfg = getConfig();
		Connection connection = DriverManager.getConnection(cfg.getUrl(),
				cfg.getUser(), cfg.getPassword());
		String initSql = getHostConfig().getConnectionInitSql();
		if (initSql != null && !"".equals(initSql)) {
			Statement statement = null;
			try {
				statement = connection.createStatement();
				statement.execute(initSql);
			} finally {
				if (statement != null) {
					statement.close();
				}
			}
		}
		return connection;
	}

}
