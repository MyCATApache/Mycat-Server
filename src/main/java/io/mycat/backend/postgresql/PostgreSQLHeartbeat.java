package io.mycat.backend.postgresql;

import io.mycat.backend.heartbeat.DBHeartbeat;

public class PostgreSQLHeartbeat extends DBHeartbeat {
	public PostgreSQLHeartbeat(PostgreSQLDataSource source) {

	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

	@Override
	public String getLastActiveTime() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getTimeout() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void heartbeat() {
		// TODO Auto-generated method stub

	}

}
