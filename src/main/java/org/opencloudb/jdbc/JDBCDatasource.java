package org.opencloudb.jdbc;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.SQLException;
import java.util.List;
import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.mysql.nio.handler.ResponseHandler;

public class JDBCDatasource extends PhysicalDatasource {
	static {
		// 加载可能的驱动
		List<String> drivers = Lists.newArrayList("com.mysql.jdbc.Driver", "org.opencloudb.jdbc.mongodb.MongoDriver", "oracle.jdbc.OracleDriver",
				"com.microsoft.sqlserver.jdbc.SQLServerDriver");
		for (String driver : drivers)
		{
			try
			{
				Class.forName(driver);
			} catch (ClassNotFoundException ignored)
			{
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
	public void createNewConnection(ResponseHandler handler,String schema) throws IOException {
		DBHostConfig cfg = getConfig();
		JDBCConnection c = new JDBCConnection();
		
		c.setHost(cfg.getIp());
		c.setPort(cfg.getPort());
		c.setPool(this);
		c.setSchema(schema);
		c.setDbType(cfg.getDbType());
		
		try {
            // TODO 这里应该有个连接池
			Connection con = getConnection();
			// c.setIdleTimeout(pool.getConfig().getIdleTimeout());
			c.setCon(con);
			// notify handler
			handler.connectionAcquired(c);
		} catch (Exception e) {
			handler.connectionError(e, c);
		}

	}

    Connection getConnection() throws SQLException
    {
        DBHostConfig cfg = getConfig();
        return DriverManager.getConnection(cfg.getUrl(), cfg.getUser(), cfg.getPassword());
    }

}
