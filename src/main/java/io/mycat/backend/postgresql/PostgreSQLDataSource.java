package io.mycat.backend.postgresql;

import io.mycat.backend.datasource.PhysicalDatasource;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.backend.postgresql.heartbeat.PostgreSQLHeartbeat;
import io.mycat.config.model.DBHostConfig;
import io.mycat.config.model.DataHostConfig;

import java.io.IOException;

/*******************
 * PostgreSQL 后端数据源实现
 * @author Coollf
 *
 */
public class PostgreSQLDataSource extends PhysicalDatasource {
	private final PostgreSQLBackendConnectionFactory factory;

	public PostgreSQLDataSource(DBHostConfig config, DataHostConfig hostConfig,
			boolean isReadNode) {
		super(config, hostConfig, isReadNode);
		this.factory = new PostgreSQLBackendConnectionFactory();
	}

	@Override
	public DBHeartbeat createHeartBeat() {
		return new PostgreSQLHeartbeat(this);
	}

	@Override
	public void createNewConnection(ResponseHandler handler, String schema)
			throws IOException {
		factory.make(this, handler, schema);
	}

	@Override
	public boolean testConnection(String schema) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

}
