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
package org.opencloudb.mysql.nio.handler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mysql.LoadDataUtil;
import org.opencloudb.net.mysql.ErrorPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;

import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.server.parser.ServerParseShow;
import org.opencloudb.server.response.ShowFullTables;
import org.opencloudb.server.response.ShowTables;

import org.opencloudb.stat.QueryResult;
import org.opencloudb.stat.QueryResultDispatcher;

import org.opencloudb.util.StringUtil;

/**
 * @author mycat
 */
public class SingleNodeHandler implements ResponseHandler, Terminatable,
		LoadDataResponseHandler {
	private static final Logger LOGGER = Logger
			.getLogger(SingleNodeHandler.class);
	private final RouteResultsetNode node;
	private final RouteResultset rrs;
	private final NonBlockingSession session;
	// only one thread access at one time no need lock
	private volatile byte packetId;
	private volatile ByteBuffer buffer;
	private volatile boolean isRunning;
	private Runnable terminateCallBack;
	private long startTime;

    private volatile boolean isDefaultNodeShowTable;
    private volatile boolean isDefaultNodeShowFullTable;
    private  Set<String> shardingTablesSet;
	
	public SingleNodeHandler(RouteResultset rrs, NonBlockingSession session) {
		this.rrs = rrs;
		this.node = rrs.getNodes()[0];
		if (node == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}
		if (session == null) {
			throw new IllegalArgumentException("session is null!");
		}
		this.session = session;
        ServerConnection source = session.getSource();
        String schema=source.getSchema();
        if(schema!=null&&ServerParse.SHOW==rrs.getSqlType())
        {
            SchemaConfig schemaConfig= MycatServer.getInstance().getConfig().getSchemas().get(schema);
           int type= ServerParseShow.tableCheck(rrs.getStatement(),0) ;
            isDefaultNodeShowTable=(ServerParseShow.TABLES==type &&!Strings.isNullOrEmpty(schemaConfig.getDataNode()));
            isDefaultNodeShowFullTable=(ServerParseShow.FULLTABLES==type &&!Strings.isNullOrEmpty(schemaConfig.getDataNode()));

            if(isDefaultNodeShowTable)
            {
                shardingTablesSet = ShowTables.getTableSet(source, rrs.getStatement());
            } else
            if(isDefaultNodeShowFullTable)
            {
                shardingTablesSet = ShowFullTables.getTableSet(source, rrs.getStatement());
            }
        }
	}

	@Override
	public void terminate(Runnable callback) {
		boolean zeroReached = false;

		if (isRunning) {
			terminateCallBack = callback;
		} else {
			zeroReached = true;
		}

		if (zeroReached) {
			callback.run();
		}
	}

	private void endRunning() {
		Runnable callback = null;
		if (isRunning) {
			isRunning = false;
			callback = terminateCallBack;
			terminateCallBack = null;
		}

		if (callback != null) {
			callback.run();
		}
	}

	private void recycleResources() {

		ByteBuffer buf = buffer;
		if (buf != null) {
			session.getSource().recycle(buffer);
			buffer = null;

		}
	}

	public void execute() throws Exception {
		//从这里开始计算处理时间
		startTime=System.currentTimeMillis();
		ServerConnection sc = session.getSource();
		this.isRunning = true;
		this.packetId = 0;
		final BackendConnection conn = session.getTarget(node);
		//之前是否获取过Connection并且Connection有效
		if (session.tryExistsCon(conn, node)) {
			_execute(conn);
		} else {
			// create new connection
			MycatConfig conf = MycatServer.getInstance().getConfig();
			//从config中获取DataNode
			PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
			//获取对应的数据库连接
			dn.getConnection(dn.getDatabase(), sc.isAutocommit(), node, this,
                    node);
		}

	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		session.bindConnection(node, conn);
		_execute(conn);

	}

	private void _execute(BackendConnection conn) {
		if (session.closed()) {
			endRunning();
			session.clearResources(true);
			return;
		}
		conn.setResponseHandler(this);
		try {
			conn.execute(node, session.getSource(), session.getSource()
					.isAutocommit());
		} catch (Exception e1) {
			executeException(conn, e1);
			return;
		}
	}

	private void executeException(BackendConnection c, Exception e) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ERR_FOUND_EXCEPION;
		err.message = StringUtil.encode(e.toString(), session.getSource()
				.getCharset());

		this.backConnectionErr(err, c);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {

		endRunning();
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_NEW_ABORTING_CONNECTION;
		err.message = StringUtil.encode(e.getMessage(), session.getSource()
				.getCharset());
		ServerConnection source = session.getSource();
		source.write(err.write(allocBuffer(), source, true));
	}

	@Override
	public void errorResponse(byte[] data, BackendConnection conn) {
		ErrorPacket err = new ErrorPacket();
		err.read(data);
		err.packetId = ++packetId;
		backConnectionErr(err, conn);

	}

	private void backConnectionErr(ErrorPacket errPkg, BackendConnection conn) {
		endRunning();
		
		ServerConnection source = session.getSource();
		String errUser = source.getUser();
		String errHost = source.getHost();
		int errPort = source.getLocalPort();
		
		String errmgs = " errno:" + errPkg.errno + " " + new String(errPkg.message);
		LOGGER.warn("execute  sql err :" + errmgs + " con:" + conn 
				+ " frontend host:" + errHost + "/" + errPort + "/" + errUser);
		
		session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(), false);
		
		source.setTxInterrupt(errmgs);
		errPkg.write(source);
		recycleResources();
	}


	@Override
	public void okResponse(byte[] data, BackendConnection conn) {        
		boolean executeResponse = conn.syncAndExcute();		
		if (executeResponse) {
			ServerConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
            boolean isCanClose2Client =(!rrs.isCallStatement()) ||(rrs.isCallStatement() &&!rrs.getProcedure().isResultSimpleValue());
			if (rrs.isLoadData()) {
				byte lastPackId = source.getLoadDataInfileHandler()
						.getLastPackId();
				ok.packetId = ++lastPackId;// OK_PACKET
				source.getLoadDataInfileHandler().clear();
			} else if(isCanClose2Client)
            {
				ok.packetId = ++packetId;// OK_PACKET
			}


            if(isCanClose2Client)   {
            session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
                    false);
            endRunning();
            }

			ok.serverStatus = source.isAutocommit() ? 2 : 1;

			recycleResources();

            if(isCanClose2Client)
            {  source.setLastInsertId(ok.insertId);
                ok.write(source);
            }
			//TODO: add by zhuam
			//查询结果派发
			QueryResult queryResult = new QueryResult(session.getSource().getUser(), session.getSource().getHost(),
					rrs.getSqlType(), rrs.getStatement(), startTime, System.currentTimeMillis());
			QueryResultDispatcher.dispatchQuery( queryResult );
 
		}
	}


	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		ServerConnection source = session.getSource();
		conn.recordSql(source.getHost(), source.getSchema(),
                node.getStatement());
        // 判断是调用存储过程的话不能在这里释放链接
		if (!rrs.isCallStatement()||(rrs.isCallStatement()&&rrs.getProcedure().isResultSimpleValue()))
        {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
			endRunning();
		}

		eof[3] = ++packetId;
		buffer = source.writeToBuffer(eof, allocBuffer());
		source.write(buffer);

	}

	/**
	 * lazy create ByteBuffer only when needed
	 * 
	 * @return
	 */
	private ByteBuffer allocBuffer() {
		if (buffer == null) {
			buffer = session.getSource().allocate();
		}
		return buffer;
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		
		
			//TODO: add by zhuam
			//查询结果派发
			QueryResult queryResult = new QueryResult(session.getSource().getUser(), session.getSource().getHost(),
					rrs.getSqlType(), rrs.getStatement(), startTime, System.currentTimeMillis());
			QueryResultDispatcher.dispatchQuery( queryResult );

            header[3] = ++packetId;
            ServerConnection source = session.getSource();
            buffer = source.writeToBuffer(header, allocBuffer());
            for (int i = 0, len = fields.size(); i < len; ++i)
            {
                byte[] field = fields.get(i);
                field[3] = ++packetId;
                buffer = source.writeToBuffer(field, buffer);
            }
            eof[3] = ++packetId;
            buffer = source.writeToBuffer(eof, buffer);
            
			if (isDefaultNodeShowTable) {
				for (String name : shardingTablesSet) {
					RowDataPacket row = new RowDataPacket(1);
					row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
					row.packetId = ++packetId;
					buffer = row.write(buffer, source, true);
				}
			}  else
            if (isDefaultNodeShowFullTable) {
                for (String name : shardingTablesSet) {
                    RowDataPacket row = new RowDataPacket(1);
                    row.add(StringUtil.encode(name.toLowerCase(), source.getCharset()));
                    row.add(StringUtil.encode("BASE TABLE", source.getCharset()));
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, source, true);
                }
            }
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
        if(isDefaultNodeShowTable||isDefaultNodeShowFullTable)
        {
            RowDataPacket rowDataPacket =new RowDataPacket(1);
            rowDataPacket.read(row);
            String table=  StringUtil.decode(rowDataPacket.fieldValues.get(0),conn.getCharset());
            if(shardingTablesSet.contains(table.toUpperCase())) return;
        }
        RowDataPacket rowDataPacket =new RowDataPacket(1);
        rowDataPacket.read(row);
            row[3] = ++packetId;
            buffer = session.getSource().writeToBuffer(row, allocBuffer());

	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		ErrorPacket err = new ErrorPacket();
		err.packetId = ++packetId;
		err.errno = ErrorCode.ER_ERROR_ON_CLOSE;
		err.message = StringUtil.encode(reason, session.getSource()
				.getCharset());
		this.backConnectionErr(err, conn);

	}

	public void clearResources() {

	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		LoadDataUtil.requestFileDataResponse(data, conn);
	}

	@Override
	public String toString() {
		return "SingleNodeHandler [node=" + node + ", packetId=" + packetId
				+ "]";
	}

}
