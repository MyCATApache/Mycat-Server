package io.mycat.backend.postgresql;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.PhysicalDatasource;
import io.mycat.net.Connection;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.executors.ResponseHandler;
import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

/*************************************************************
 * PostgreSQL Native Connection impl
 * @author Coollf
 *
 */
public class PostgreSQLBackendConnection extends Connection implements BackendConnection{
	/**
	 * 来自子接口
	 */
	private boolean fromSlaveDB;
	
	/***
	 * 用户名
	 */
	private String user;
	
	/**
	 * 密码
	 */
	private String password;
	
	/***
	 * 对应数据库空间
	 */
	private String schema;
	
	
	/**
	 * 数据源配置
	 */
	private PostgreSQLDataSource pool;
	private Object attachment;	
	protected volatile String charset;
	private volatile boolean autocommit;
	private long currentTimeMillis;
	
	/***
	 * 响应handler
	 */
	private ResponseHandler respHandler;
	private boolean borrowed;
	private volatile int txIsolation;
	private volatile boolean modifiedSQLExecuted = false;
	private long lastTime;
	private AtomicBoolean isQuit;
	

	public PostgreSQLBackendConnection(SocketChannel channel, boolean fromSlaveDB) {
		super(channel);
		this.fromSlaveDB = fromSlaveDB;
		this.lastTime = TimeUtil.currentTimeMillis();
		this.isQuit = new AtomicBoolean(false);
		this.autocommit = true;
	}

	@Override
	public boolean isFromSlaveDB() {
		return fromSlaveDB;
	}

	@Override
	public String getSchema() {
		return schema;
	}

	@Override
	public void setSchema(String newSchema) {
		this.schema = newSchema;		
	}


	@Override
	public void setAttachment(Object attachment) {
		this.attachment = attachment;	
	}


	@Override
	public void setLastTime(long currentTimeMillis) {
		this.currentTimeMillis = currentTimeMillis;
	}



	@Override
	public void setResponseHandler(ResponseHandler queryHandler) {
		this.respHandler =  queryHandler;
	}



	@Override
	public Object getAttachment() {
		return attachment;
	}



	@Override
	public boolean isBorrowed() {
		return borrowed;
	}

	@Override
	public void setBorrowed(boolean borrowed) {
		this.lastTime = TimeUtil.currentTimeMillis();
		this.borrowed = borrowed;
	}

	@Override
	public int getTxIsolation() {
		return txIsolation;
	}

	@Override
	public boolean isAutocommit() {
		return autocommit;
	}


	@Override
	public String getCharset() {
		return charset;
	}

	@Override
	public PhysicalDatasource getPool() {
		return pool;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @param fromSlaveDB the fromSlaveDB to set
	 */
	public void setFromSlaveDB(boolean fromSlaveDB) {
		this.fromSlaveDB = fromSlaveDB;
	}

	/**
	 * @param pool the pool to set
	 */
	public void setPool(PostgreSQLDataSource pool) {
		this.pool = pool;
	}

	@Override
	public boolean isModifiedSQLExecuted() {
		return  modifiedSQLExecuted ;
	}

	@Override
	public long getLastTime() {
		return lastTime;
	}

	@Override
	public boolean isClosedOrQuit() {
		return isClosed() || isQuit.get();
	}

	@Override
	public void quit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void release() {
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
	public void execute(RouteResultsetNode node, MySQLFrontConnection source,
			boolean autocommit) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean syncAndExcute() {
//		StatusSync sync = this.statusSync;
//		if (sync == null) {
//			return true;
//		} else {
//			boolean executed = sync.synAndExecuted(this);
//			if (executed) {
//				statusSync = null;
//			}
//			return executed;
//		}
		return false;
	}

	@Override
	public void rollback() {
		// TODO Auto-generated method stub
		
	}

}
