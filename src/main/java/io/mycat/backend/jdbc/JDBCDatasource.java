package io.mycat.backend.jdbc;

import io.mycat.backend.PhysicalDatasource;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.net.ConnectIdGenerator;
import io.mycat.server.config.loader.LocalLoader;
import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.config.node.JdbcDriver;
import io.mycat.server.executors.ResponseHandler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCDatasource extends PhysicalDatasource {
	public static final Logger logger = LoggerFactory.getLogger(JDBCDatasource.class);
	public static Map<String, JdbcDriver> jdbcDriverConfig = LocalLoader.loadJdbcDriverConfig();

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
		JdbcDriver driver = jdbcDriverConfig.get(cfg.getDbType().toLowerCase());	// 获取对应 dbType 的驱动 className
		try {
			if(driver != null)
				Class.forName(driver.getClassName());	
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			logger.error("createNewConnection error " + e.getMessage());
			return;
		}
		
		JDBCConnection c = null;
		try {
			// TODO: 这里需要实现连继池
			Connection con = getConnection();
			c = new JDBCConnection();
			c.setHost(cfg.getIp());
			c.setPort(cfg.getPort());
			c.setPool(this);
			c.setSchema(schema);
			c.setDbType(cfg.getDbType());
			c.setId(ConnectIdGenerator.getINSTNCE().getId()); // 复用mysql的Backend的ID，需要在process中存储
			
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
