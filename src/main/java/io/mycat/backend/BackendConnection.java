package io.mycat.backend;

import io.mycat.net.ClosableConnection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.executors.ResponseHandler;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public interface BackendConnection extends ClosableConnection{
	public boolean isModifiedSQLExecuted();

	public boolean isFromSlaveDB();

	public String getSchema();

	public void setSchema(String newSchema);

	public long getLastTime();

	public boolean isClosedOrQuit();

	public void setAttachment(Object attachment);

	public void quit();

	public void setLastTime(long currentTimeMillis);

	public void release();

	public void setResponseHandler(ResponseHandler commandHandler);

	public void commit();

	public void query(String sql) throws UnsupportedEncodingException;

	public Object getAttachment();

	// public long getThreadId();

	public void execute(RouteResultsetNode node, MySQLFrontConnection source,
			boolean autocommit) throws IOException;

	public boolean syncAndExcute();

	public void rollback();

	public boolean isBorrowed();

	public void setBorrowed(boolean borrowed);

	public int getTxIsolation();

	public boolean isAutocommit();

	public long getId();

	public void close(String reason);

	public String getCharset();

	public PhysicalDatasource getPool();
}
