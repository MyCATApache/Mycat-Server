/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.heartbeat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.NetworkChannel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.opencloudb.config.Capabilities;
import org.opencloudb.mysql.SecurityUtil;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.BackendAIOConnection;
import org.opencloudb.net.mysql.AuthPacket;
import org.opencloudb.net.mysql.CommandPacket;
import org.opencloudb.net.mysql.HandshakePacket;
import org.opencloudb.net.mysql.MySQLPacket;
import org.opencloudb.net.mysql.QuitPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public class MySQLDetector extends BackendAIOConnection {
	private static final Logger LOGGER = Logger.getLogger(MySQLDetector.class);
	private static final long CLIENT_FLAGS = initClientFlags();

	private MySQLHeartbeat heartbeat;
	private final long clientFlags;
	private HandshakePacket handshake;
	private int charsetIndex;
	private boolean isAuthenticated;
	private String user;
	private String password;
	private String schema;
	private long heartbeatTimeout;
	private final AtomicBoolean isQuit;

	public MySQLDetector(NetworkChannel channel) {
		super(channel);
		this.clientFlags = CLIENT_FLAGS;
		this.handler = new MySQLDetectorAuthenticator(this);
		this.isQuit = new AtomicBoolean(false);
	}

	public MySQLHeartbeat getHeartbeat() {
		return heartbeat;
	}

	public void setHeartbeat(MySQLHeartbeat heartbeat) {
		this.heartbeat = heartbeat;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getSchema() {
		return schema;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public long getHeartbeatTimeout() {
		return heartbeatTimeout;
	}

	public void setHeartbeatTimeout(long heartbeatTimeout) {
		this.heartbeatTimeout = heartbeatTimeout;
	}

	public boolean isHeartbeatTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
				lastReadTime) + heartbeatTimeout;
	}

	public long lastReadTime() {
		return lastReadTime;
	}

	public long lastWriteTime() {
		return lastWriteTime;
	}

	public boolean isAuthenticated() {
		return isAuthenticated;
	}

	public void setAuthenticated(boolean isAuthenticated) {
		this.isAuthenticated = isAuthenticated;
	}

	public HandshakePacket getHandshake() {
		return handshake;
	}

	public void setHandshake(HandshakePacket handshake) {
		this.handshake = handshake;
	}

	public void setCharsetIndex(int charsetIndex) {
		this.charsetIndex = charsetIndex;
	}

	public void authenticate() {
		AuthPacket packet = new AuthPacket();
		packet.packetId = 1;
		packet.clientFlags = clientFlags;
		packet.maxPacketSize = maxPacketSize;
		packet.charsetIndex = charsetIndex;
		packet.user = user;
		try {
			packet.password = getPass(password, handshake);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e.getMessage());
		}
		packet.database = schema;
		packet.write(this);
	}

	public void heartbeat() {
		if (isAuthenticated) {
			String sql = heartbeat.getHeartbeatSQL();
			if (sql != null) {
				CommandPacket packet = new CommandPacket();
				packet.packetId = 0;
				packet.command = MySQLPacket.COM_QUERY;
				packet.arg = sql.getBytes();
				packet.write(this);
			}
		} else {
			// System.out.println("auth ");
			authenticate();
		}
	}

	public void quit() {
		if (isQuit.compareAndSet(false, true)) {
			if (isAuthenticated) {
				write(writeToBuffer(QuitPacket.QUIT, allocate()));
				write(allocate());
				close("heart beat quit normal");
			} else {
				close("heartbeat quit");
			}
		}
	}

	public boolean isQuit() {
		return isQuit.get();
	}

	@Override
	public void idleCheck() {
		if (isIdleTimeout()) {
			LOGGER.warn(toString() + " heatbeat idle timeout");
			quit();
		}
	}

	public String toString() {
		return new StringBuilder().append("[thread=")
				.append(Thread.currentThread().getName()).append(",class=")
				.append(getClass().getSimpleName()).append(",host=")
				.append(host).append(",port=").append(port)
				.append(",localPort=").append(localPort).append(",schema=")
				.append(schema).append(']').toString();
	}

	private static long initClientFlags() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		// flag |= Capabilities.CLIENT_COMPRESS;
		flag |= Capabilities.CLIENT_ODBC;
		 flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= Capabilities.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
		// client extension
		// flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
		// flag |= Capabilities.CLIENT_MULTI_RESULTS;
		return flag;
	}

	private static byte[] getPass(String src, HandshakePacket hsp)
			throws NoSuchAlgorithmException {
		if (src == null || src.length() == 0) {
			return null;
		}
		byte[] passwd = src.getBytes();
		int sl1 = hsp.seed.length;
		int sl2 = hsp.restOfScrambleBuff.length;
		byte[] seed = new byte[sl1 + sl2];
		System.arraycopy(hsp.seed, 0, seed, 0, sl1);
		System.arraycopy(hsp.restOfScrambleBuff, 0, seed, sl1, sl2);
		return SecurityUtil.scramble411(passwd, seed);
	}

	@Override
	public void onConnectFailed(Throwable e) {
		heartbeat.setResult(MySQLHeartbeat.ERROR_STATUS, this, true,
				"hearbeat connecterr");

	}

	@Override
	public boolean isModifiedSQLExecuted() {
		return false;
	}

	@Override
	public boolean isFromSlaveDB() {
		return false;
	}

	@Override
	public long getLastTime() {
		return 0;
	}

	@Override
	public boolean isClosedOrQuit() {
		return isQuit.get();
	}

	@Override
	public void setAttachment(Object attachment) {

	}

	@Override
	public void setLastTime(long currentTimeMillis) {

	}

	@Override
	public void release() {

	}

	@Override
	public boolean setResponseHandler(ResponseHandler commandHandler) {
		return false;
	}

	@Override
	public void commit() {

	}

	@Override
	public void query(String sql) throws UnsupportedEncodingException {

	}

	@Override
	public Object getAttachment() {
		return null;
	}

	@Override
	public void execute(RouteResultsetNode node, ServerConnection source,
			boolean autocommit) throws IOException {

	}

	@Override
	public void recordSql(String host, String schema, String statement) {

	}

	@Override
	public boolean syncAndExcute() {
		return false;
	}

	@Override
	public void rollback() {

	}

	@Override
	public boolean isBorrowed() {
		return false;
	}

	@Override
	public void setBorrowed(boolean borrowed) {

	}

	@Override
	public int getTxIsolation() {
		return 0;
	}

	@Override
	public boolean isAutocommit() {
		return false;
	}

}