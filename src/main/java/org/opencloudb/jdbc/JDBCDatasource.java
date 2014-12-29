package org.opencloudb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;

import org.opencloudb.backend.PhysicalDatasource;
import org.opencloudb.config.model.DBHostConfig;
import org.opencloudb.config.model.DataHostConfig;
import org.opencloudb.heartbeat.DBHeartbeat;
import org.opencloudb.mysql.nio.handler.ResponseHandler;

public class JDBCDatasource extends PhysicalDatasource {
	public JDBCDatasource(DBHostConfig config, DataHostConfig hostConfig,
			boolean isReadNode) {
		super(config, hostConfig, isReadNode);

	}

	@Override
	public DBHeartbeat createHeartBeat() {
		return new JDBCHeatbeat();
	}

	@Override
	public void createNewConnection(ResponseHandler handler,String schema) throws IOException {
		DBHostConfig dsc = getConfig();
		JDBCConnection c = new JDBCConnection();

		c.setHost(dsc.getIp());
		c.setPort(dsc.getPort());
		c.setPool(this);
		c.setSchema(schema);
		
		String dbtype=dsc.getDbType();
		try {
			if (dbtype.equals("mysql")) {
				Class.forName( "com.mysql.jdbc.Driver" );					
			}
			else if (dbtype.equals("mongodb")) {
				Class.forName( "org.opencloudb.jdbc.mongodb.MongoDriver" );				
			}else if (dbtype.equals("oracle")){
				Class.forName( "oracle.jdbc.OracleDriver" );				
			}			
			Connection con = DriverManager.getConnection(dsc.getUrl(),dsc.getUser(),dsc.getPassword());
			// c.setIdleTimeout(pool.getConfig().getIdleTimeout());
			c.setCon(con);
			// notify handler
			handler.connectionAcquired(c);
		} catch (Exception e) {
			handler.connectionError(e, c);
		}

	}

}
