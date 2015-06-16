package io.mycat.jdbc;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.config.ErrorCode;
import io.mycat.mysql.nio.handler.ConnectionHeartBeatHandler;
import io.mycat.mysql.nio.handler.ResponseHandler;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.ErrorPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.MysqlDefs;
import io.mycat.util.ResultSetUtil;
import io.mycat.util.StringUtil;
import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.log4j.Logger;

public class JDBCConnection implements BackendConnection {
	protected static final Logger LOGGER = Logger
			.getLogger(JDBCConnection.class);
	private JDBCDatasource pool;
	private volatile String schema;
	private volatile String dbType;
	private volatile String oldSchema;
	private byte packetId;
	private int txIsolation;
	private volatile boolean running = false;
	private volatile boolean borrowed;
	private long id = 0;
	private String host;
	private int port;
	private Connection con;
	private ResponseHandler respHandler;
	private volatile Object attachement;

	boolean headerOutputed = false;
	private volatile boolean modifiedSQLExecuted;
	private final long startTime;
	private long lastTime;
	private boolean isSpark = false;

	private NIOProcessor processor;
	
	
	
	public NIOProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(NIOProcessor processor) {
        this.processor = processor;
    }

    public JDBCConnection() {
		startTime = System.currentTimeMillis();
	}

	public Connection getCon() {
		return con;
	}

	public void setCon(Connection con) {
		this.con = con;

	}

	@Override
	public void close(String reason) {
		try {
			con.close();
			if(processor!=null){
			    processor.removeConnection(this);
			}
			
		} catch (SQLException e) {
		}

	}

	public void setId(long id) {
        this.id = id;
    }
	
	public JDBCDatasource getPool() {
        return pool;
    }

    public void setPool(JDBCDatasource pool) {
		this.pool = pool;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public boolean isClosed() {
		try {
			return con == null || con.isClosed();
		} catch (SQLException e) {
			return true;
		}
	}

	@Override
	public void idleCheck() {
	    if(TimeUtil.currentTimeMillis() > lastTime + pool.getConfig().getIdleTimeout()){
	        close(" idle  check");
	    }
	}

	@Override
	public long getStartupTime() {
		return startTime;
	}

	@Override
	public String getHost() {
		return this.host;
	}

	@Override
	public int getPort() {
		return this.port;
	}

	@Override
	public int getLocalPort() {
		return 0;
	}

	@Override
	public long getNetInBytes() {

		return 0;
	}

	@Override
	public long getNetOutBytes() {
		return 0;
	}

	@Override
	public boolean isModifiedSQLExecuted() {
		return modifiedSQLExecuted;
	}

	@Override
	public boolean isFromSlaveDB() {
		return false;
	}

	public String getDbType() {
		return this.dbType;
	}

	public void setDbType(String newDbType) {
		this.dbType = newDbType.toUpperCase();
		this.isSpark = dbType.equals("SPARK");

	}

	@Override
	public String getSchema() {
		return this.schema;
	}

	@Override
	public void setSchema(String newSchema) {
		this.oldSchema = this.schema;
		this.schema = newSchema;

	}

	@Override
	public long getLastTime() {

		return lastTime;
	}

	@Override
	public boolean isClosedOrQuit() {
		return this.isClosed();
	}

	@Override
	public void setAttachment(Object attachment) {
		this.attachement = attachment;

	}

	@Override
	public void quit() {
		this.close("client quit");

	}

	@Override
	public void setLastTime(long currentTimeMillis) {
		this.lastTime = currentTimeMillis;

	}

	@Override
	public void release() {
		modifiedSQLExecuted = false;
		setResponseHandler(null);
		pool.releaseChannel(this);
	}

	public void setRunning(boolean running) {
		this.running = running;

	}

	@Override
	public boolean setResponseHandler(ResponseHandler commandHandler) {
		respHandler = commandHandler;
		return false;
	}

	@Override
	public void commit() {
		try {
			con.commit();

			this.respHandler.okResponse(OkPacket.OK, this);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void executeSQL(RouteResultsetNode rrn, ServerConnection sc,
							boolean autocommit) throws IOException {
		String orgin = rrn.getStatement();
		// String sql = rrn.getStatement().toLowerCase();
		// LOGGER.info("JDBC SQL:"+orgin+"|"+sc.toString());
		if (!modifiedSQLExecuted && rrn.isModifySQL()) {
			modifiedSQLExecuted = true;
		}

		try {
			if (!this.schema.equals(this.oldSchema)) {
				con.setCatalog(schema);
				this.oldSchema = schema;
			}
			if (!this.isSpark) {
				con.setAutoCommit(autocommit);
			}
			int sqlType = rrn.getSqlType();

			if (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW) {
				if ((sqlType == ServerParse.SHOW) && (!dbType.equals("MYSQL"))) {
					// showCMD(sc, orgin);
					//ShowVariables.execute(sc, orgin);
					ShowVariables.execute(sc, orgin,this);
				} else if ("SELECT CONNECTION_ID()".equalsIgnoreCase(orgin)) {
					//ShowVariables.justReturnValue(sc,String.valueOf(sc.getId()));
					ShowVariables.justReturnValue(sc,String.valueOf(sc.getId()),this);
				} else {
					ouputResultSet(sc, orgin);
				}
			} else {
				executeddl(sc, orgin);
			}

		} catch (SQLException e) {

			String msg = e.getMessage();
			ErrorPacket error = new ErrorPacket();
			error.packetId = ++packetId;
			error.errno = e.getErrorCode();
			error.message = msg.getBytes();
			this.respHandler.errorResponse(error.writeToBytes(sc), this);
		}
		catch (Exception e) {
			String msg = e.getMessage();
			ErrorPacket error = new ErrorPacket();
			error.packetId = ++packetId;
			error.errno = ErrorCode.ER_UNKNOWN_ERROR;
			error.message = msg.getBytes();
			this.respHandler.errorResponse(error.writeToBytes(sc), this);
		}
		finally {
			this.running = false;
		}

	}

	private FieldPacket getNewFieldPacket(String charset, String fieldName) {
		FieldPacket fieldPacket = new FieldPacket();
		fieldPacket.orgName = StringUtil.encode(fieldName, charset);
		fieldPacket.name = StringUtil.encode(fieldName, charset);
		fieldPacket.length = 20;
		fieldPacket.flags = 0;
		fieldPacket.decimals = 0;
		int javaType = 12;
		fieldPacket.type = (byte) (MysqlDefs.javaTypeMysql(javaType) & 0xff);
		return fieldPacket;
	}

	private void executeddl(ServerConnection sc, String sql)
			throws SQLException {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			int count = stmt.executeUpdate(sql);
			OkPacket okPck = new OkPacket();
			okPck.affectedRows = count;
			okPck.insertId = 0;
			okPck.packetId = ++packetId;
			okPck.message = " OK!".getBytes();
			this.respHandler.okResponse(okPck.writeToBytes(sc), this);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {

				}
			}
		}
	}

	private void ouputResultSet(ServerConnection sc, String sql)
			throws SQLException {
		ResultSet rs = null;
		Statement stmt = null;

		try {
			stmt = con.createStatement();
			rs = stmt.executeQuery(sql);

			List<FieldPacket> fieldPks = new LinkedList<FieldPacket>();
			ResultSetUtil.resultSetToFieldPacket(sc.getCharset(), fieldPks, rs,
					this.isSpark);
			int colunmCount = fieldPks.size();
			ByteBuffer byteBuf = sc.allocate();
			ResultSetHeaderPacket headerPkg = new ResultSetHeaderPacket();
			headerPkg.fieldCount = fieldPks.size();
			headerPkg.packetId = ++packetId;

			byteBuf = headerPkg.write(byteBuf, sc, true);
			byteBuf.flip();
			byte[] header = new byte[byteBuf.limit()];
			byteBuf.get(header);
			byteBuf.clear();
			List<byte[]> fields = new ArrayList<byte[]>(fieldPks.size());
			Iterator<FieldPacket> itor = fieldPks.iterator();
			while (itor.hasNext()) {
				FieldPacket curField = itor.next();
				curField.packetId = ++packetId;
				byteBuf = curField.write(byteBuf, sc, false);
				byteBuf.flip();
				byte[] field = new byte[byteBuf.limit()];
				byteBuf.get(field);
				byteBuf.clear();
				fields.add(field);
				itor.remove();
			}
			EOFPacket eofPckg = new EOFPacket();
			eofPckg.packetId = ++packetId;
			byteBuf = eofPckg.write(byteBuf, sc, false);
			byteBuf.flip();
			byte[] eof = new byte[byteBuf.limit()];
			byteBuf.get(eof);
			byteBuf.clear();
			this.respHandler.fieldEofResponse(header, fields, eof, this);

			// output row
			while (rs.next()) {
				RowDataPacket curRow = new RowDataPacket(colunmCount);
				for (int i = 0; i < colunmCount; i++) {
					int j = i + 1;
					curRow.add(StringUtil.encode(rs.getString(j),
							sc.getCharset()));
				}
				curRow.packetId = ++packetId;
				byteBuf = curRow.write(byteBuf, sc, false);
				byteBuf.flip();
				byte[] row = new byte[byteBuf.limit()];
				byteBuf.get(row);
				byteBuf.clear();
				this.respHandler.rowResponse(row, this);
			}

			// end row
			eofPckg = new EOFPacket();
			eofPckg.packetId = ++packetId;
			byteBuf = eofPckg.write(byteBuf, sc, false);
			byteBuf.flip();
			eof = new byte[byteBuf.limit()];
			byteBuf.get(eof);
			sc.recycle(byteBuf);
			this.respHandler.rowEofResponse(eof, this);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {

				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {

				}
			}
		}
	}

	@Override
	public void query(final String sql) throws UnsupportedEncodingException {
		if(respHandler instanceof ConnectionHeartBeatHandler)
		{
			justForHeartbeat(sql);
		}    else
		{
			throw new UnsupportedEncodingException("unsupported yet ");
		}
	}
	private void justForHeartbeat(String sql)
			  {

		Statement stmt = null;

		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			if(!isAutocommit()){ //如果在写库上，如果是事务方式的连接，需要进行手动commit
			    con.commit();
			}
			this.respHandler.okResponse(OkPacket.OK, this);

		}
		catch (Exception e)
		{
			String msg = e.getMessage();
			ErrorPacket error = new ErrorPacket();
			error.packetId = ++packetId;
			error.errno = ErrorCode.ER_UNKNOWN_ERROR;
			error.message = msg.getBytes();
			this.respHandler.errorResponse(error.writeToBytes(), this);
		}
		finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {

				}
			}
		}
	}
	@Override
	public Object getAttachment() {
		return this.attachement;
	}

	@Override
	public String getCharset() {
		return null;
	}

	@Override
	public void execute(final RouteResultsetNode node,
						final ServerConnection source, final boolean autocommit)
			throws IOException {
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				try {
					executeSQL(node, source, autocommit);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};

		MycatServer.getInstance().getBusinessExecutor().execute(runnable);
	}

	@Override
	public void recordSql(String host, String schema, String statement) {

	}

	@Override
	public boolean syncAndExcute() {
		return true;
	}

	@Override
	public void rollback() {
		try {
			con.rollback();

			this.respHandler.okResponse(OkPacket.OK, this);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isRunning() {
		return this.running;
	}

	@Override
	public boolean isBorrowed() {
		return this.borrowed;
	}

	@Override
	public void setBorrowed(boolean borrowed) {
		this.borrowed = borrowed;

	}

	@Override
	public int getTxIsolation() {
		if (con != null) {
			try {
				return con.getTransactionIsolation();
			} catch (SQLException e) {
				return 0;
			}
		} else {
			return -1;
		}
	}

	@Override
	public boolean isAutocommit() {
		if (con == null) {
			return true;
		} else {
			try {
				return con.getAutoCommit();
			} catch (SQLException e) {

			}
		}
		return true;
	}

	@Override
	public long getId() {
		return id;
	}

	@Override
    public String toString() {
        return "JDBCConnection [id=" + id +",autocommit="+this.isAutocommit()+",pool=" + pool + ", schema=" + schema + ", dbType=" + dbType + ", oldSchema="
                + oldSchema + ", packetId=" + packetId + ", txIsolation=" + txIsolation + ", running=" + running
                + ", borrowed=" + borrowed + ", host=" + host + ", port=" + port + ", con=" + con
                + ", respHandler=" + respHandler + ", attachement=" + attachement + ", headerOutputed="
                + headerOutputed + ", modifiedSQLExecuted=" + modifiedSQLExecuted + ", startTime=" + startTime
                + ", lastTime=" + lastTime + ", isSpark=" + isSpark + ", processor=" + processor + "]";
    }
	
	

}
