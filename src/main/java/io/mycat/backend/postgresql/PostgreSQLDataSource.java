package io.mycat.backend.postgresql;

import java.io.IOException;

import io.mycat.backend.PhysicalDatasource;
import io.mycat.backend.heartbeat.DBHeartbeat;
import io.mycat.backend.postgresql.heartbeat.PostgreSQLHeartbeat;
import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.config.node.DataHostConfig;
import io.mycat.server.executors.ResponseHandler;

public class PostgreSQLDataSource extends PhysicalDatasource {
	private final PostgreSQLBackendConnectionFactory factory;
	
	public PostgreSQLDataSource(DBHostConfig config, DataHostConfig hostConfig, boolean isReadNode) {
		super(config, hostConfig, isReadNode);
		this.factory = new PostgreSQLBackendConnectionFactory();
	}

	@Override
	public DBHeartbeat createHeartBeat() {
		return new PostgreSQLHeartbeat(this);
	}

	@Override
	public void createNewConnection(ResponseHandler handler, String schema) throws IOException {
		factory.make(this, handler,schema);
	}

}
