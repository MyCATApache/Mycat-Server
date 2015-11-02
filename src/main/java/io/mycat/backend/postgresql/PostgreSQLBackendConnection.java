package io.mycat.backend.postgresql;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.SocketChannel;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.net.Connection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.executors.ResponseHandler;

/*************************************************************
 * PostgreSQL Native Connection impl
 * @author Coollf
 *
 */
public class PostgreSQLBackendConnection extends Connection implements BackendConnection{

	public PostgreSQLBackendConnection(SocketChannel channel) {
		super(channel);
	}

	@Override
	public boolean isClosed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void idleCheck() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getStartupTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getHost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getLocalPort() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNetInBytes() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNetOutBytes() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isModifiedSQLExecuted() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFromSlaveDB() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSchema(String newSchema) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getLastTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isClosedOrQuit() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setAttachment(Object attachment) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void quit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setLastTime(long currentTimeMillis) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void release() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setResponseHandler(ResponseHandler commandHandler) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void query(String sql) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getAttachment() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(RouteResultsetNode node, MySQLFrontConnection source, boolean autocommit) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean syncAndExcute() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isBorrowed() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setBorrowed(boolean borrowed) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getTxIsolation() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isAutocommit() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long getId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close(String reason) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getCharset() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PhysicalDatasource getPool() {
		// TODO Auto-generated method stub
		return null;
	}

}
