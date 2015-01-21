package org.opencloudb.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.opencloudb.backend.BackendConnection;
import org.opencloudb.mysql.nio.handler.ResponseHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.ResultSetUtil;

public class JDBCConnection implements BackendConnection {
	private JDBCDatasource pool;
	private volatile String schema;
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
		} catch (SQLException e) {
		}

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
		// TODO Auto-generated method stub

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
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void executeSQL(RouteResultsetNode rrn, ServerConnection sc,
			boolean autocommit) throws IOException {
		String orgin = rrn.getStatement();
		String sql = rrn.getStatement().toLowerCase();
		try {
			if (!this.schema.equals(this.oldSchema)) {
				con.setCatalog(schema);
				this.oldSchema = schema;
			}
			con.setAutoCommit(autocommit);
			int sqlType = rrn.getSqlType();
			if (sqlType == ServerParse.SELECT || sqlType == ServerParse.SHOW) {	
				//非Mysql数据库不能识别SHOW命令
				if (sqlType == ServerParse.SELECT) {
					ouputResultSet(sc, orgin);
				}
			} else {
				executeddl(sc, sql);
			}

		} catch (SQLException e) {
			e.printStackTrace();

			String msg = e.getMessage();
			ErrorPacket error = new ErrorPacket();
			error.packetId = ++packetId;
			error.errno = e.getErrorCode();
			error.message = msg.getBytes();
			this.respHandler.errorResponse(error.writeToBytes(sc), this);
		} finally {
			this.running = false;
		}

	}

	private void executeddl(ServerConnection sc, String sql)
			throws SQLException {
		Statement stmt = null;
		try {
			int count = con.createStatement().executeUpdate(sql);
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
			List<RowDataPacket> rowsPkg = new LinkedList<RowDataPacket>();
			List<FieldPacket> fieldPks = new LinkedList<FieldPacket>();
			ResultSetUtil.resultSetToPacket(sc.getCharset(), con, fieldPks, rs,
					rowsPkg);
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
			Iterator<RowDataPacket> rowItor = rowsPkg.iterator();
			while (rowItor.hasNext()) {
				RowDataPacket curRow = rowItor.next();
				curRow.packetId = ++packetId;
				rowItor.remove();
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
	public void query(String sql) throws UnsupportedEncodingException {

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
	public void execute(RouteResultsetNode node, ServerConnection source,
			boolean autocommit) throws IOException {
		executeSQL(node, source, autocommit);

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
		return "JDBCConnection [autocommit=" + this.isAutocommit()
				+ ", txIsolation=" + txIsolation + ", running=" + running
				+ ", borrowed=" + borrowed + ", id=" + id + ", host=" + host
				+ ", port=" + port + "]";
	}

}
