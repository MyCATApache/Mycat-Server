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
package io.mycat.net;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.backend.mysql.MySQLMessage;
import io.mycat.config.Capabilities;
import io.mycat.config.ErrorCode;
import io.mycat.config.Versions;
import io.mycat.net.handler.FrontendAuthenticator;
import io.mycat.net.handler.FrontendPrepareHandler;
import io.mycat.net.handler.FrontendPrivileges;
import io.mycat.net.handler.FrontendQueryHandler;
import io.mycat.net.handler.LoadDataInfileHandler;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.HandshakePacket;
import io.mycat.net.mysql.HandshakeV10Packet;
import io.mycat.net.mysql.MySQLPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.CompressUtil;
import io.mycat.util.RandomUtil;

/**
 * @author mycat
 */
public abstract class FrontendConnection extends AbstractConnection {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FrontendConnection.class);

	protected long idleTimeout;
	protected byte[] seed;
	protected String user;
	protected String schema;
	protected String executeSql;

	protected volatile long executeSqlId = 0;
	protected AtomicLong  responseSqlId = new AtomicLong(0); //新增executeSqlId ,repsonseSqlId 用于避免对一个sql 写回了多个错误的结果.
	
	protected FrontendPrivileges privileges;
	protected FrontendQueryHandler queryHandler;
	protected FrontendPrepareHandler prepareHandler;
	protected LoadDataInfileHandler loadDataInfileHandler;
	protected boolean isAccepted;
	protected boolean isAuthenticated;

	public FrontendConnection(NetworkChannel channel) throws IOException {
		super(channel);
		InetSocketAddress localAddr = (InetSocketAddress) channel.getLocalAddress();
		InetSocketAddress remoteAddr = null;
		if (channel instanceof SocketChannel) {
			remoteAddr = (InetSocketAddress) ((SocketChannel) channel).getRemoteAddress();	
			
		} else if (channel instanceof AsynchronousSocketChannel) {
			remoteAddr = (InetSocketAddress) ((AsynchronousSocketChannel) channel).getRemoteAddress();
		}
		
		this.host = remoteAddr.getHostString();
		this.port = localAddr.getPort();
		this.localPort = remoteAddr.getPort();
		this.handler = new FrontendAuthenticator(this);
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
		if(LOGGER.isDebugEnabled()) {
			LOGGER.debug(this + " localPort:" + this.localPort + " port"+this.port);		
		}
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getLocalPort() {
		return localPort;
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public void setAccepted(boolean isAccepted) {
		this.isAccepted = isAccepted;
	}

	public void setProcessor(NIOProcessor processor) {
		super.setProcessor(processor);
		processor.addFrontend(this);
	}

	public LoadDataInfileHandler getLoadDataInfileHandler() {
		return loadDataInfileHandler;
	}

	public void setLoadDataInfileHandler(LoadDataInfileHandler loadDataInfileHandler) {
		this.loadDataInfileHandler = loadDataInfileHandler;
	}

	public void setQueryHandler(FrontendQueryHandler queryHandler) {
		this.queryHandler = queryHandler;
	}

	public void setPrepareHandler(FrontendPrepareHandler prepareHandler) {
		this.prepareHandler = prepareHandler;
	}

	public void setAuthenticated(boolean isAuthenticated) {
		this.isAuthenticated = isAuthenticated;
	}

	public FrontendPrivileges getPrivileges() {
		return privileges;
	}

	public void setPrivileges(FrontendPrivileges privileges) {
		this.privileges = privileges;
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

	public String getExecuteSql() {
		return executeSql;
	}

	public void setExecuteSql(String executeSql) {
		this.executeSql = executeSql;
	}

	public byte[] getSeed() {
		return seed;
	}

	public boolean setCharsetIndex(int ci) {
		String charset = CharsetUtil.getCharset(ci);
		if (charset != null) {
			return setCharset(charset);
		} else {
			return false;
		}
	}

	public void writeErrMessage(int errno, String msg) {
		if(this.canResponse()){
				LOGGER.error("{}{} write errorMsg:{} error",this, msg+ getStack());
			writeErrMessage((byte) 1, errno, msg);
		} else {
				LOGGER.error("{} write errorMsg:{} error",this,msg);
		}
	}

	//前端sql已经返回结果集 则调用这个函数 避免向前端返回多次结果。
	//modify by zwy
	public void setResponseId() {
		//this.responseSqlId = this.executeSqlId;
		this.canResponse();
	}
	public String getStack() {
		StackTraceElement stack[] = Thread.currentThread().getStackTrace();  
		StringBuilder sb = new StringBuilder();
        for(int i=0;i<stack.length;i++){
        	sb.append(stack[i].getClassName()+" ."+stack[i].getMethodName()+stack[i].getLineNumber()+"\n");
        }
        return sb.toString();
	}
	//modify by zwy 2018.07
	public boolean canResponse() {
		
//			return true;
		long resId = this.responseSqlId.get();
		if(this.executeSqlId > resId) {
			boolean t = this.responseSqlId.compareAndSet(resId, this.executeSqlId); 
			if(false) {
				StackTraceElement stack[] = Thread.currentThread().getStackTrace();  
				StringBuilder sb = new StringBuilder();
                for(int i=0;i<stack.length;i++){
                	sb.append(stack[i].getClassName()+" ."+stack[i].getMethodName()+stack[i].getLineNumber()+"\n");
                }				
				LOGGER.debug("can Response " + this.toString() + "  "+ getStack());
			}
			return t;
		}else {
			return false;
		}
	}
	public void writeErrMessage(byte id, int errno, String msg) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = id;
		err.errno = errno;
		err.message = encodeString(msg, charset);
		err.write(this);
	}
	
	public void initDB(byte[] data) {
		
		MySQLMessage mm = new MySQLMessage(data);
		mm.position(5);
		String db = mm.readString();

		// 检查schema的有效性
		if (db == null || !privileges.schemaExists(db)) {
			writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
			return;
		}
		
		if (!privileges.userExists(user, host)) {
			writeErrMessage(ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + user + "'");
			return;
		}
		
		Set<String> schemas = privileges.getUserSchemas(user);
		if (schemas == null || schemas.size() == 0 || schemas.contains(db)) {
			this.schema = db;
			write(writeToBuffer(OkPacket.OK, allocate()));
		} else {
			String s = "Access denied for user '" + user + "' to database '" + db + "'";
			writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
		}
	}


	public void loadDataInfileStart(String sql) {
		if (loadDataInfileHandler != null) {
			try {
				loadDataInfileHandler.start(sql);
			} catch (Exception e) {
				LOGGER.error("load data error", e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
			}

		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile sql is not  unsupported!");
		}
	}

	public void loadDataInfileData(byte[] data) {
		if (loadDataInfileHandler != null) {
			try {
				loadDataInfileHandler.handle(data);
			} catch (Exception e) {
				LOGGER.error("load data error", e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
			}
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile  data is not  unsupported!");
		}

	}

	public void loadDataInfileEnd(byte packID) {
		if (loadDataInfileHandler != null) {
			try {
				loadDataInfileHandler.end(packID);
			} catch (Exception e) {
				LOGGER.error("load data error", e);
				writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.getMessage());
			}
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "load data infile end is not  unsupported!");
		}
	}
	
	
	public void query(String sql) {
		
		if (sql == null || sql.length() == 0) {
			writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
			return;
		}
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(new StringBuilder().append(this).append(" ").append(sql).toString());
		}
		
		// remove last ';'
		if (sql.endsWith(";")) {
			sql = sql.substring(0, sql.length() - 1);
		}
		// remove like '/* ApplicationName=DBeaver 6.0.1 - Main */' tool app hints
		if(sql.indexOf("/* ApplicationName") >=0) {
			sql = sql.replaceFirst("\\/\\*.*\\*\\/\\s*", "");
		}
		
		// 记录SQL
		this.setExecuteSql(sql);
		
		// 防火墙策略( SQL 黑名单/ 注入攻击)
		if ( !privileges.checkFirewallSQLPolicy( user, sql ) ) {
			writeErrMessage(ErrorCode.ERR_WRONG_USED, 
					"The statement is unsafe SQL, reject for user '" + user + "'");
			return;
		}		
		
		// DML 权限检查
		try {
			boolean isPassed = privileges.checkDmlPrivilege(user, schema, sql);
			if ( !isPassed ) {
				writeErrMessage(ErrorCode.ERR_WRONG_USED, 
						"The statement DML privilege check is not passed, reject for user '" + user + "'");
				return;
			}
		 } catch( com.alibaba.druid.sql.parser.ParserException e1) {
	        	writeErrMessage(ErrorCode.ERR_WRONG_USED,  e1.getMessage());
	        	LOGGER.error("parse exception", e1 );
				return;
	     }
		
		// 执行查询
		if (queryHandler != null) {			
			queryHandler.setReadOnly(privileges.isReadOnly(user));
			queryHandler.query(sql);
			
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Query unsupported!");
		}		
	}
	
	public void query(byte[] data) {
		
		// 取得语句
		String sql = null;		
		try {
			MySQLMessage mm = new MySQLMessage(data);
			mm.position(5);
			sql = mm.readString(charset);
		} catch (UnsupportedEncodingException e) {
			writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
			return;
		}		
		
		this.query( sql );
	}

	public void stmtPrepare(byte[] data) {
		if (prepareHandler != null) {
			// 取得语句
			MySQLMessage mm = new MySQLMessage(data);
			mm.position(5);
			String sql = null;
			try {
				sql = mm.readString(charset);
			} catch (UnsupportedEncodingException e) {
				writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET,
						"Unknown charset '" + charset + "'");
				return;
			}
			if (sql == null || sql.length() == 0) {
				writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
				return;
			}
			
			// 记录SQL
			this.setExecuteSql(sql);
			
			// 执行预处理
			prepareHandler.prepare(sql);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
		}
	}
	
	public void stmtSendLongData(byte[] data) {
		if(prepareHandler != null) {
			prepareHandler.sendLongData(data);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
		}
	}
	
	public void stmtReset(byte[] data) {
		if(prepareHandler != null) {
			prepareHandler.reset(data);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
		}
	}

	public void stmtExecute(byte[] data) {
		if (prepareHandler != null) {
			prepareHandler.execute(data);
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
		}
	}

	public void stmtClose(byte[] data) {
		if (prepareHandler != null) {
			prepareHandler.close( data );
		} else {
			writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
		}
	}
	/** 
	 * 用来模拟mysql协议命令中的com_field_list，方便通过发sql测试 
	 * https://dev.mysql.com/doc/internals/en/com-field-list.html
	 */
	public void fieldList(byte[] data) {
		// 取得语句
		String sql = null;		
		try {
			MySQLMessage mm = new MySQLMessage(data);
			mm.position(5);
			sql = mm.readString(charset);
			sql = ServerParse.COM_FIELD_LIST_FLAG + sql;
		} catch (UnsupportedEncodingException e) {
			writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
			return;
		}		
		this.query( sql );
	}

	public void ping() {
		write(writeToBuffer(OkPacket.OK, allocate()));
	}

	public void heartbeat(byte[] data) {
		write(writeToBuffer(OkPacket.OK, allocate()));
	}

	public void kill(byte[] data) {
		close("kill command");
	}

	public void unknown(byte[] data) {
		writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
	}

	@Override
	public void register() throws IOException {
		if (!isClosed.get()) {

			// 生成认证数据
			byte[] rand1 = RandomUtil.randomBytes(8);
			byte[] rand2 = RandomUtil.randomBytes(12);

			// 保存认证数据
			byte[] seed = new byte[rand1.length + rand2.length];
			System.arraycopy(rand1, 0, seed, 0, rand1.length);
			System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
			this.seed = seed;

			// 发送握手数据包
			boolean useHandshakeV10 = MycatServer.getInstance().getConfig().getSystem().getUseHandshakeV10() == 1;
			if(useHandshakeV10) {
				HandshakeV10Packet hs = new HandshakeV10Packet();
				hs.packetId = 0;
				hs.protocolVersion = Versions.PROTOCOL_VERSION;
				hs.serverVersion = Versions.SERVER_VERSION;
				hs.threadId = id;
				hs.seed = rand1;
				hs.serverCapabilities = getServerCapabilities();
				hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
				hs.serverStatus = 2;
				hs.restOfScrambleBuff = rand2;
				hs.write(this);
			} else {
				HandshakePacket hs = new HandshakePacket();
				hs.packetId = 0;
				hs.protocolVersion = Versions.PROTOCOL_VERSION;
				hs.serverVersion = Versions.SERVER_VERSION;
				hs.threadId = id;
				hs.seed = rand1;
				hs.serverCapabilities = getServerCapabilities();
				hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
				hs.serverStatus = 2;
				hs.restOfScrambleBuff = rand2;
				hs.write(this);
			}

			// asynread response
			this.asynRead();
		}
	}

	@Override
	public void handle(final byte[] data) {
		this.executeSqlId ++;
		if (isSupportCompress()) {			
			List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, decompressUnfinishedDataQueue);
			for (byte[] pack : packs) {
				if (pack.length != 0) {
					rawHandle(pack);
				}
			}
			
		} else {
			rawHandle(data);
		}
	}

	public void rawHandle(final byte[] data) {

		//load data infile  客户端会发空包 长度为4
		if (data.length == 4 && data[0] == 0 && data[1] == 0 && data[2] == 0) {
			// load in data空包
			loadDataInfileEnd(data[3]);
			return;
		}
		//修改quit的判断,当load data infile 分隔符为\001 时可能会出现误判断的bug.
		if (data.length>4 && data[0] == 1 && data[1] == 0 && data[2]== 0 && data[3] == 0 &&data[4] == MySQLPacket.COM_QUIT) {
			this.getProcessor().getCommands().doQuit();
			this.close("quit cmd");
			return;
		}
		handler.handle(data);
	}

	protected int getServerCapabilities() {
		int flag = 0;
		flag |= Capabilities.CLIENT_LONG_PASSWORD;
		flag |= Capabilities.CLIENT_FOUND_ROWS;
		flag |= Capabilities.CLIENT_LONG_FLAG;
		flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
		// flag |= Capabilities.CLIENT_NO_SCHEMA;
		boolean usingCompress= MycatServer.getInstance().getConfig().getSystem().getUseCompression()==1 ;
		if (usingCompress) {
			flag |= Capabilities.CLIENT_COMPRESS;
		}
		
		flag |= Capabilities.CLIENT_ODBC;
		 flag |= Capabilities.CLIENT_LOCAL_FILES;
		flag |= Capabilities.CLIENT_IGNORE_SPACE;
		flag |= Capabilities.CLIENT_PROTOCOL_41;
		flag |= Capabilities.CLIENT_INTERACTIVE;
		// flag |= Capabilities.CLIENT_SSL;
		flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
		flag |= Capabilities.CLIENT_TRANSACTIONS;
		// flag |= ServerDefs.CLIENT_RESERVED;
		flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        boolean useHandshakeV10 = MycatServer.getInstance().getConfig().getSystem().getUseHandshakeV10() == 1;
        if(useHandshakeV10) {
        	flag |= Capabilities.CLIENT_PLUGIN_AUTH;
        }
		return flag;
	}

	protected boolean isConnectionReset(Throwable t) {
		if (t instanceof IOException) {
			String msg = t.getMessage();
			return (msg != null && msg.contains("Connection reset by peer"));
		}
		return false;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("[thread=")
				.append(Thread.currentThread().getName()).append(",class=")
				.append(getClass().getSimpleName()).append(",id=").append(id)
				.append(",host=").append(host).append(",port=").append(port)
				.append(",schema=").append(schema).append(']').toString();
	}

	private final static byte[] encodeString(String src, String charset) {
		if (src == null) {
			return null;
		}
		if (charset == null) {
			return src.getBytes();
		}
		try {
			return src.getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			return src.getBytes();
		}
	}

	@Override
	public void close(String reason) {
		super.close(isAuthenticated ? reason : "");
	}
}