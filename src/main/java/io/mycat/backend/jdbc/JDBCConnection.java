package io.mycat.backend.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.route.Procedure;
import io.mycat.route.ProcedureParameter;
import io.mycat.util.*;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.nio.handler.ConnectionHeartBeatHandler;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.Isolations;
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

public class JDBCConnection implements BackendConnection {
	protected static final Logger LOGGER = LoggerFactory
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
	private boolean setSchemaFail = false;
	
	
	
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
			try {
				if (!isAutocommit()) {
					rollback();
					con.setAutoCommit(true);
				}
			}catch (Exception e){
				LOGGER.error("close jdbc connection, found it is in transcation so try to rollback");
			}
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
    private  int convertNativeIsolationToJDBC(int nativeIsolation)
    {
        if(nativeIsolation== Isolations.REPEATED_READ)
        {
            return Connection.TRANSACTION_REPEATABLE_READ;
        }else
        if(nativeIsolation== Isolations.SERIALIZABLE)
        {
            return Connection.TRANSACTION_SERIALIZABLE;
        } else
        {
            return nativeIsolation;
        }
    }



    private void syncIsolation(int nativeIsolation)
    {
        int jdbcIsolation=convertNativeIsolationToJDBC(nativeIsolation);
        int srcJdbcIsolation=   getTxIsolation();
		if (jdbcIsolation == srcJdbcIsolation || "oracle".equalsIgnoreCase(getDbType())
				&& jdbcIsolation != Connection.TRANSACTION_READ_COMMITTED
				&& jdbcIsolation != Connection.TRANSACTION_SERIALIZABLE) {
			return;
		}
		try
        {
            con.setTransactionIsolation(jdbcIsolation);
        } catch (SQLException e)
        {
            LOGGER.warn("set txisolation error:",e);
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
            syncIsolation(sc.getTxIsolation()) ;
			if (!this.schema.equals(this.oldSchema)) {
				con.setCatalog(schema);
				if (!setSchemaFail) {
                    try {
                        con.setSchema(schema); //add@byron to test
                    } catch (Throwable e) {
                        LOGGER.error("JDBC setSchema Exception for " + schema, e);
                        setSchemaFail = true;
                    }
                }
				this.oldSchema = schema;
			}
			if (!this.isSpark) {
				con.setAutoCommit(autocommit);
			}
			int sqlType = rrn.getSqlType();
             if(rrn.isCallStatement()&&"oracle".equalsIgnoreCase(getDbType()))
             {
                 //存储过程暂时只支持oracle
                 ouputCallStatement(rrn,sc,orgin);
             }  else
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
			error.message = ((msg == null) ? e.toString().getBytes() : msg.getBytes());
			String err = null;
			if(error.message!=null){
			    err = new String(error.message);
			}
			LOGGER.error("sql execute error, "+ err , e);
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
			int count = stmt.executeUpdate(sql,Statement.RETURN_GENERATED_KEYS);
			long lastInsertId = 0;
			ResultSet generatedKeys = stmt.getGeneratedKeys();
			if (generatedKeys != null){
				ResultSetMetaData metaData = generatedKeys.getMetaData();
				if (metaData.getColumnCount() == 1){
					lastInsertId = (generatedKeys.next() ? generatedKeys.getLong(1) : 0L);
				}
			}
			OkPacket okPck = new OkPacket();
			okPck.affectedRows = count;
			okPck.insertId = lastInsertId;
			okPck.packetId = ++packetId;
			okPck.message = " OK!".getBytes();
			this.respHandler.okResponse(okPck.writeToBytes(sc), this);
		}catch (Exception e){
			LOGGER.error("",e);
			throw e;
		}finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					LOGGER.error("",e);
				}
			}
		}
	}


    private static int oracleCURSORTypeValue=-10;
    static
    {
        Object cursor = ObjectUtil.getStaticFieldValue("oracle.jdbc.OracleTypes", "CURSOR");
        if(cursor!=null) {
			oracleCURSORTypeValue = (int) cursor;
		}
    }
	private void ouputCallStatement(RouteResultsetNode rrn,ServerConnection sc, String sql)
			throws SQLException {

        CallableStatement stmt = null;
        ResultSet rs = null;
		try {
            Procedure procedure = rrn.getProcedure();
            Collection<ProcedureParameter> paramters=    procedure.getParamterMap().values();
            String callSql = procedure.toPreCallSql(null);
            stmt = con.prepareCall(callSql);

            for (ProcedureParameter paramter : paramters)
            {
                if((ProcedureParameter.IN.equalsIgnoreCase(paramter.getParameterType())
                        ||ProcedureParameter.INOUT.equalsIgnoreCase(paramter.getParameterType())))
                {
                  Object value=  paramter.getValue()!=null ?paramter.getValue():paramter.getName();
                    stmt.setObject(paramter.getIndex(),value);
                }

                if(ProcedureParameter.OUT.equalsIgnoreCase(paramter.getParameterType())
                        ||ProcedureParameter.INOUT.equalsIgnoreCase(paramter.getParameterType())  )
                {
                    int jdbcType ="oracle".equalsIgnoreCase(getDbType())&& procedure.getListFields().contains(paramter.getName())?oracleCURSORTypeValue: paramter.getJdbcType();
                    stmt.registerOutParameter(paramter.getIndex(), jdbcType);
                }
            }

            boolean hadResults= stmt.execute();

            ByteBuffer byteBuf = sc.allocate();
            if(procedure.getSelectColumns().size()>0&&!procedure.isResultList())
            {
                List<FieldPacket> fieldPks = new LinkedList<FieldPacket>();
                for (ProcedureParameter paramter : paramters)
                {
                    if (!procedure.getListFields().contains(paramter.getName())&&(ProcedureParameter.OUT.equalsIgnoreCase(paramter.getParameterType())
                            || ProcedureParameter.INOUT.equalsIgnoreCase(paramter.getParameterType()))   )
                    {
                        FieldPacket packet = PacketUtil.getField(paramter.getName(), MysqlDefs.javaTypeMysql(paramter.getJdbcType()));
                        fieldPks.add(packet);
                    }
                }
                int colunmCount = fieldPks.size();

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
                RowDataPacket curRow = new RowDataPacket(colunmCount);
                for (String name : procedure.getSelectColumns())
                {
                    ProcedureParameter procedureParameter=   procedure.getParamterMap().get(name);
					Object object = stmt.getObject(procedureParameter.getIndex());
					if (object != null){
						curRow.add(StringUtil.encode(String.valueOf(object),
								sc.getCharset()));
					}else {
						curRow.add(null);
					}
                }

                curRow.packetId = ++packetId;
                byteBuf = curRow.write(byteBuf, sc, false);
                byteBuf.flip();
                byte[] row = new byte[byteBuf.limit()];
                byteBuf.get(row);
                byteBuf.clear();
                this.respHandler.rowResponse(row, this);

                eofPckg = new EOFPacket();
                eofPckg.packetId = ++packetId;
                if(procedure.isResultList())
                {
                    eofPckg.status = 42;
                }
                byteBuf = eofPckg.write(byteBuf, sc, false);
                byteBuf.flip();
                eof = new byte[byteBuf.limit()];
                byteBuf.get(eof);
                byteBuf.clear();
                this.respHandler.rowEofResponse(eof, this);
            }


            if(procedure.isResultList())
            {
                List<FieldPacket> fieldPks = new LinkedList<FieldPacket>();
                int listSize=procedure.getListFields().size();
                for (ProcedureParameter paramter : paramters)
                {
                    if (procedure.getListFields().contains(paramter.getName())&&(ProcedureParameter.OUT.equalsIgnoreCase(paramter.getParameterType())
                            || ProcedureParameter.INOUT.equalsIgnoreCase(paramter.getParameterType()))  )
                    {
                        listSize--;

                        Object object = stmt.getObject(paramter.getIndex());
                        rs= (ResultSet) object;
                        if(rs==null) {
							continue;
						}
                        ResultSetUtil.resultSetToFieldPacket(sc.getCharset(), fieldPks, rs,
                                this.isSpark);

                        int colunmCount = fieldPks.size();
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
								Object object1 = rs.getObject(j);
								if (object1 == null){
									curRow.add(null);
								}else {
									curRow.add(StringUtil.encode(Objects.toString(object1),
											sc.getCharset()));
								}
                            }
                            curRow.packetId = ++packetId;
                            byteBuf = curRow.write(byteBuf, sc, false);
                            byteBuf.flip();
                            byte[] row = new byte[byteBuf.limit()];
                            byteBuf.get(row);
                            byteBuf.clear();
                            this.respHandler.rowResponse(row, this);
                        }
                        eofPckg = new EOFPacket();
                        eofPckg.packetId = ++packetId;
                        if(listSize!=0)
                        {
                            eofPckg.status = 42;
                        }
                        byteBuf = eofPckg.write(byteBuf, sc, false);
                        byteBuf.flip();
                        eof = new byte[byteBuf.limit()];
                        byteBuf.get(eof);
                        byteBuf.clear();
                        this.respHandler.rowEofResponse(eof, this);
                    }
                }

            }



            if(!procedure.isResultSimpleValue())
            {
                byte[] OK = new byte[] { 7, 0, 0, 1, 0, 0, 0, 2, 0, 0,
                        0 };
                OK[3]=++packetId;
                this.respHandler.okResponse(OK,this);
            }
            sc.recycle(byteBuf);
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
					if(MysqlDefs.isBianry((byte) fieldPks.get(i).type)) {
							curRow.add(rs.getBytes(j));
					} else if(fieldPks.get(i).type == MysqlDefs.FIELD_TYPE_DECIMAL ||
							fieldPks.get(i).type == (MysqlDefs.FIELD_TYPE_NEW_DECIMAL - 256)) { // field type is unsigned byte
						// ensure that do not use scientific notation format
						BigDecimal val = rs.getBigDecimal(j);
						curRow.add(StringUtil.encode(val != null ? val.toPlainString() : null,
								sc.getCharset()));
					} else {
						   curRow.add(StringUtil.encode(rs.getString(j),
								   sc.getCharset()));
					}

				}
				curRow.packetId = ++packetId;
				byteBuf = curRow.write(byteBuf, sc, false);
				byteBuf.flip();
				byte[] row = new byte[byteBuf.limit()];
				byteBuf.get(row);
				byteBuf.clear();
				this.respHandler.rowResponse(row, this);
			}

			fieldPks.clear();

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
			throw new UnsupportedOperationException("global seq is not unsupported in jdbc driver yet ");
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

	@Override
	public void discardClose(String reason) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void query(String sql, int charsetIndex) {
		try {
			query(sql);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			LOGGER.debug("UnsupportedEncodingException :"+ e.getMessage());
		}		
	}

	@Override
	public boolean checkAlive() {
		try {
			return !con.isClosed();
		} catch (SQLException e) {
			LOGGER.error("connection is closed",e);
			return false;
		}
	}


}
