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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCDatasource extends PhysicalDatasource {
	public static final Logger logger = LoggerFactory.getLogger(JDBCDatasource.class);
	private static Map<String, JdbcDriver> jdbcDriverConfig = null;
	
	static { // 最多也就3,4个数据库，一次性加载驱动类
		jdbcDriverConfig = LocalLoader.loadJdbcDriverConfig();
		if(jdbcDriverConfig != null && jdbcDriverConfig.size() > 0){
			for(String key : jdbcDriverConfig.keySet()){
				JdbcDriver driver = jdbcDriverConfig.get(key);
				if(driver != null && StringUtils.isNotBlank(driver.getClassName())){
					try {
						Class.forName(driver.getClassName());
					} catch (ClassNotFoundException e) {
						logger.error("Class.forName load jdbcDriver for "+key+" error: " + e.getMessage());
					}	
				}else{
					logger.error(" driver for " + key + " is not exist or className has no value,"
							+ " please check jdbcDriver-config element in mycat.xml.");
				}
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
		Enumeration<Driver> drivers = DriverManager.getDrivers();
		Driver d = drivers.nextElement();
		d.getClass().getName();
		Connection connection = DriverManager.getConnection(cfg.getUrl(),
				cfg.getUser(), cfg.getPassword());
		String initSql = getHostConfig().getConnectionInitSql();
		if (StringUtils.isNotBlank(initSql)) {
			try (Statement statement = connection.createStatement()){
				statement.execute(initSql);
			} catch(SQLException e) {
				logger.warn(" getConnection error: " + e.getMessage());
			}
		}
		return connection;
	}
	
	/**
	 * 根据 dbType 获取 JdbcDriver
	 * @param dbType mysql
	 * @return JdbcDriver: {'mysql':'com.mysql.jdbc.Driver'}
	 */
	public static JdbcDriver getJdbcDriverBydbType(String dbType){
		if(StringUtils.isNotBlank(dbType)){
			return jdbcDriverConfig.get(dbType.toLowerCase());	// 获取对应 dbType 的 JdbcDriver
		}
		return null;
	}

	public static Map<String, JdbcDriver> getJdbcDriverConfig() {
		return jdbcDriverConfig;
	}

	public static void setJdbcDriverConfig(Map<String, JdbcDriver> jdbcDriverConfig) {
		JDBCDatasource.jdbcDriverConfig = jdbcDriverConfig;
	}

}
