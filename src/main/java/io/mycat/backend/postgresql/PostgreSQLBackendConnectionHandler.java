package io.mycat.backend.postgresql;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.mycat.net.NIOHandler;

public class PostgreSQLBackendConnectionHandler implements NIOHandler<PostgreSQLBackendConnection> {

	@Override
	public void onConnected(PostgreSQLBackendConnection con) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectFailed(PostgreSQLBackendConnection con, Throwable e) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onClosed(PostgreSQLBackendConnection con, String reason) {
		// TODO Auto-generated method stub

	}

	@Override
	public void handle(PostgreSQLBackendConnection con, ByteBuffer data, int start, int readedLength) {
		// TODO Auto-generated method stub

	}

}
