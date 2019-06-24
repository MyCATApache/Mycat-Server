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
package io.mycat.backend.mysql.nio.handler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.LoadDataUtil;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.MycatConfig;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.net.mysql.BinaryRowDataPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.sqlengine.mpp.AbstractDataNodeMerge;
import io.mycat.sqlengine.mpp.ColMeta;
import io.mycat.sqlengine.mpp.DataMergeService;
import io.mycat.sqlengine.mpp.DataNodeMergeManager;
import io.mycat.sqlengine.mpp.MergeCol;
import io.mycat.statistic.stat.QueryResult;
import io.mycat.statistic.stat.QueryResultDispatcher;
import io.mycat.util.ResultSetUtil;

/**
 * @author mycat
 */
public class MultiNodeQueryHandler extends MultiNodeHandler implements LoadDataResponseHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(MultiNodeQueryHandler.class);

	private final RouteResultset rrs;
	private final NonBlockingSession session;
	// private final CommitNodeHandler icHandler;
	private final AbstractDataNodeMerge dataMergeSvr;
	private final boolean autocommit;
	private String priamaryKeyTable = null;
	private int primaryKeyIndex = -1;
	private int fieldCount = 0;
	private final ReentrantLock lock;
	private long affectedRows;
	private long selectRows;
	private long insertId;
	private volatile boolean fieldsReturned;
	private int okCount;
	private final boolean isCallProcedure;
	private long startTime;
	private long netInBytes;
	private long netOutBytes;
	private int execCount = 0;

	private boolean prepared;
	private List<FieldPacket> fieldPackets = new ArrayList<FieldPacket>();
	private int isOffHeapuseOffHeapForMerge = 1;
	//huangyiming add  中间处理结果是否处理完毕
	private final AtomicBoolean isMiddleResultDone;
	/**
	 * Limit N，M
	 */
	private   int limitStart;
	private   int limitSize;

	private int index = 0;

	private int end = 0;

	//huangyiming
	private byte[] header = null;
	private List<byte[]> fields = null;

	// by kaiz : 为了解决Mybatis获取由Mycat生成自增主键时，MySQL返回的Last_insert_id为最大值的问题；
	//		当逻辑表设置了autoIncrement='false'时，MyCAT会将ok packet当中的最小last insert id记录下来，返回给应用
	// 		当逻辑表设置了autoIncrement='true'时，MyCAT会将ok packet当中的最大的last insert id记录下来，然后减掉affected rows的数量后，返回给应用

	public MultiNodeQueryHandler(int sqlType, RouteResultset rrs,
			boolean autocommit, NonBlockingSession session) {

		super(session);
 		this.isMiddleResultDone = new AtomicBoolean(false);

		if (rrs.getNodes() == null) {
			throw new IllegalArgumentException("routeNode is null!");
		}

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("execute mutinode query " + rrs.getStatement());
		}

		this.rrs = rrs;
		isOffHeapuseOffHeapForMerge = MycatServer.getInstance().
				getConfig().getSystem().getUseOffHeapForMerge();
		if (ServerParse.SELECT == sqlType && rrs.needMerge()) {
			/**
			 * 使用Off Heap
			 */
			if(isOffHeapuseOffHeapForMerge == 1){
				dataMergeSvr = new DataNodeMergeManager(this,rrs,isMiddleResultDone);
			}else {
				dataMergeSvr = new DataMergeService(this,rrs);
			}
		} else {
			dataMergeSvr = null;
		}

		isCallProcedure = rrs.isCallStatement();
		this.autocommit = session.getSource().isAutocommit();
		this.session = session;
		this.lock = new ReentrantLock();
		// this.icHandler = new CommitNodeHandler(session);

		this.limitStart = rrs.getLimitStart();
		this.limitSize = rrs.getLimitSize();
		this.end = limitStart + rrs.getLimitSize();

		if (this.limitStart < 0)
			this.limitStart = 0;

		if (rrs.getLimitSize() < 0)
			end = Integer.MAX_VALUE;
		if ((dataMergeSvr != null)
				&& LOGGER.isDebugEnabled()) {
				LOGGER.debug("has data merge logic ");
		}

		if ( rrs != null && rrs.getStatement() != null) {
			netInBytes += rrs.getStatement().getBytes().length;
		}
	}

	protected void reset(int initCount) {
		super.reset(initCount);
		this.okCount = initCount;
		this.execCount = 0;
		this.netInBytes = 0;
		this.netOutBytes = 0;
	}

	public NonBlockingSession getSession() {
		return session;
	}

	public void execute() throws Exception {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			this.reset(rrs.getNodes().length);
			this.fieldsReturned = false;
			this.affectedRows = 0L;
			this.insertId = 0L;
		} finally {
			lock.unlock();
		}
		MycatConfig conf = MycatServer.getInstance().getConfig();
		startTime = System.currentTimeMillis();
		LOGGER.debug("rrs.getRunOnSlave()-" + rrs.getRunOnSlaveDebugInfo());
		//todo 增加处理如果超过最大链接的处理。是zwy 2018.07
		int start = 0;
		try {
			for (final RouteResultsetNode node : rrs.getNodes()) {
				BackendConnection conn = session.getTarget(node);
				if (session.tryExistsCon(conn, node)) {
					if(LOGGER.isDebugEnabled()) {
						LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
			            LOGGER.debug(new StringBuilder(this.toString()).append(session.getSource()).append(rrs).toString());
					}
					node.setRunOnSlave(rrs.getRunOnSlave());	// 实现 master/slave注解	
					if(LOGGER.isDebugEnabled()) {
						LOGGER.debug("node.getRunOnSlave()-" + node.getRunOnSlave());
					}
					_execute(conn, node);
				} else {
					// create new connection
					//LOGGER.debug("node.getRunOnSlave()1-" + node.getRunOnSlave());
					node.setRunOnSlave(rrs.getRunOnSlave());	// 实现 master/slave注解
					//LOGGER.debug("node.getRunOnSlave()2-" + node.getRunOnSlave());
					PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
					dn.getConnection(dn.getDatabase(), autocommit, node, this, node);
					// 注意该方法不仅仅是获取连接，获取新连接成功之后，会通过层层回调，最后回调到本类 的connectionAcquired
					// 这是通过 上面方法的 this 参数的层层传递完成的。
					// connectionAcquired 进行执行操作:
					// session.bindConnection(node, conn);
					// _execute(conn, node); 
				}
				start++;
			}
		}catch (Exception e) {
			ServerConnection source = session.getSource();
            int len = rrs.getNodes().length - start;
            for(int i = 0 ; i < len ; i++) {
            	//flag = this.decrementCountBy(1);
            	 this.connectionError(e, null);
            }
            LOGGER.error(new StringBuilder(this.toString()).append(source).append(rrs).toString(), e);
           // this.connectionError(e, null);
           // if(flag) {
           //     LOGGER.error(new StringBuilder(this.toString()).append(source).append(rrs).toString(), e);
            //}
		}
	}

	private void _execute(BackendConnection conn, RouteResultsetNode node) {
		if (clearIfSessionClosed(session)) {
			return;
		}
		conn.setResponseHandler(this);
		try {
			conn.execute(node, session.getSource(), autocommit);
		} catch (IOException e) {
			connectionError(e, conn);
		}
	}

	@Override
	public void connectionAcquired(final BackendConnection conn) {
		final RouteResultsetNode node = (RouteResultsetNode) conn
				.getAttachment();
		session.bindConnection(node, conn);
		if(errorRepsponsed.get()) {
			ServerConnection source = session.getSource();			
			LOGGER.warn(new StringBuilder(this.toString()).append(source).append(rrs).toString(), "connectionAcquired",conn);
			this.connectionClose(conn, "find error, so close this connection");
			return ;
		}
		_execute(conn, node);
	}

	private boolean decrementOkCountBy(int finished) {
		lock.lock();
		try {
			return --okCount == 0;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {

		this.netOutBytes += data.length;

		boolean executeResponse = conn.syncAndExcute();
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("received ok response ,executeResponse:"
					+ executeResponse + " from " + conn);
		}
		if (executeResponse) {

			ServerConnection source = session.getSource();
			OkPacket ok = new OkPacket();
			ok.read(data);
            //存储过程
            boolean isCanClose2Client =(!rrs.isCallStatement()) ||(rrs.isCallStatement() &&!rrs.getProcedure().isResultSimpleValue());;
             if(!isCallProcedure)
             {
                 if (clearIfSessionClosed(session))
                 {
                     return;
                 } else if (canClose(conn, false))
                 {
                     return;
                 }
             }
			lock.lock();
			try {
				// 判断是否是全局表，如果是，执行行数不做累加，以最后一次执行的为准。
				if (!rrs.isGlobalTable()) {
					affectedRows += ok.affectedRows;
				} else {
					affectedRows = ok.affectedRows;
				}

				if (ok.insertId > 0) {
					if (rrs.getAutoIncrement()) {
						insertId = (insertId == 0) ? ok.insertId : Math.max(
								insertId, ok.insertId);
					} else {
                        insertId = (insertId == 0) ? ok.insertId : Math.min(
                                insertId, ok.insertId);
					}
				}


			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				lock.unlock();
			}
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug(this.toString() +"on row okResponse " + conn + "  "+ errorRepsponsed.get() +"  "+nodeCount);
			}
			// 对于存储过程，其比较特殊，查询结果返回EndRow报文以后，还会再返回一个OK报文，才算结束
			boolean isEndPacket = isCallProcedure ? decrementOkCountBy(1): decrementCountBy(1);
			if (isEndPacket && isCanClose2Client) {

				if (this.autocommit && !session.getSource().isLocked()) {// clear all connections
					session.releaseConnections(false);
				}

				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}

				lock.lock();
				try {
					if (rrs.isLoadData()) {
						byte lastPackId = source.getLoadDataInfileHandler()
								.getLastPackId();
						ok.packetId = ++lastPackId;// OK_PACKET
						ok.message = ("Records: " + affectedRows + "  Deleted: 0  Skipped: 0  Warnings: 0")
								.getBytes();// 此处信息只是为了控制台给人看的
						source.getLoadDataInfileHandler().clear();
					} else {
						ok.packetId = ++packetId;// OK_PACKET
					}

					ok.affectedRows = affectedRows;
					ok.serverStatus = source.isAutocommit() ? 2 : 1;
					if (insertId > 0) {
						ok.insertId = rrs.getAutoIncrement() ? (insertId - affectedRows + 1) : insertId;
						source.setLastInsertId(insertId);
					}
					//  判断是否已经报错返回给前台了 2018.07 
					if(source.canResponse()) {
						ok.write(source);
					}
				} catch (Exception e) {
					handleDataProcessException(e);
				} finally {
					lock.unlock();
				}
			}


			// add by lian
			// 解决sql统计中写操作永远为0
			execCount++;
			if (execCount == rrs.getNodes().length) {
				source.setExecuteSql(null);  //完善show @@connection.sql 监控命令.已经执行完的sql 不再显示
				QueryResult queryResult = new QueryResult(session.getSource().getUser(),
						rrs.getSqlType(), rrs.getStatement(), selectRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(),0);
				QueryResultDispatcher.dispatchQuery( queryResult );
			}
		}
	}

	@Override
	public void rowEofResponse(final byte[] eof, BackendConnection conn) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(this.toString() +"on row end reseponse " + conn + "  "+ errorRepsponsed.get() +"  "+nodeCount);
		}

		this.netOutBytes += eof.length;
		MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();

		if (errorRepsponsed.get()) {
			// the connection has been closed or set to "txInterrupt" properly
			//in tryErrorFinished() method! If we close it here, it can
			// lead to tx error such as blocking rollback tx for ever.
			// @author Uncle-pan
			// @since 2016-03-25
			// conn.close(this.error);
			return;
		}

		final ServerConnection source = session.getSource();
		if (!isCallProcedure) {
			if (clearIfSessionClosed(session)) {
				return;
			} else if (canClose(conn, false)) {
				return;
			}
		}

		if (decrementCountBy(1)) {
            if (!rrs.isCallStatement()||(rrs.isCallStatement()&&rrs.getProcedure().isResultSimpleValue())) {
				if (this.autocommit && !session.getSource().isLocked()) {// clear all connections
					session.releaseConnections(false);
				}

				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}
			}
			if (dataMergeSvr != null) {
				//huangyiming add 数据合并前如果有中间过程则先执行数据合并再执行下一步
				if(session.getMiddlerResultHandler() !=null  ){
					isMiddleResultDone.set(true);
            	}

				try {
					dataMergeSvr.outputMergeResult(session, eof);
				} catch (Exception e) {
					handleDataProcessException(e);
				}

			} else {
				try {
					lock.lock();
					eof[3] = ++packetId;
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("last packet id:" + packetId);
					}
					if(  middlerResultHandler ==null ){
						//middlerResultHandler.secondEexcute();
						if(source.canResponse()) {
							source.write(eof);
						}
					}
 				} finally {
					lock.unlock();

				}
			}
		}
		execCount++;
		if(middlerResultHandler !=null){
			if (execCount != rrs.getNodes().length) {

				return;
			}
			/*else{
				middlerResultHandler.secondEexcute(); 
			}*/
		}
 		if (execCount == rrs.getNodes().length) {
			int resultSize = source.getWriteQueue().size()*MycatServer.getInstance().getConfig().getSystem().getBufferPoolPageSize();
			source.setExecuteSql(null);  //完善show @@connection.sql 监控命令.已经执行完的sql 不再显示
			//TODO: add by zhuam
			//查询结果派发
			QueryResult queryResult = new QueryResult(session.getSource().getUser(),
					rrs.getSqlType(), rrs.getStatement(), selectRows, netInBytes, netOutBytes, startTime, System.currentTimeMillis(),resultSize);
			QueryResultDispatcher.dispatchQuery( queryResult );


			//	add huangyiming  如果是中间过程,必须等数据合并好了再进行下一步语句的拼装
 			if(middlerResultHandler !=null ){
 				while (!this.isMiddleResultDone.compareAndSet(false, true)) {
 	                Thread.yield();
 	             }
 				middlerResultHandler.secondEexcute();
				isMiddleResultDone.set(false);
			}
		}

	}

	/**
	 * 将汇聚结果集数据真正的发送给Mycat客户端
	 * @param source
	 * @param eof
	 * @param
	 */
	public void outputMergeResult(final ServerConnection source, final byte[] eof, Iterator<UnsafeRow> iter,AtomicBoolean isMiddleResultDone) {

		try {
			lock.lock();
			ByteBuffer buffer = session.getSource().allocate();
			final RouteResultset rrs = this.dataMergeSvr.getRrs();

			/**
			 * 处理limit语句的start 和 end位置，将正确的结果发送给
			 * Mycat 客户端
			 */
			int start = rrs.getLimitStart();
			int end = start + rrs.getLimitSize();
			int index = 0;

			if (start < 0)
				start = 0;

			if (rrs.getLimitSize() < 0)
				end = Integer.MAX_VALUE;

			if(prepared) {
 				while (iter.hasNext()){
					UnsafeRow row = iter.next();
					if(index >= start){
						row.packetId = ++packetId;
						BinaryRowDataPacket binRowPacket = new BinaryRowDataPacket();
						binRowPacket.read(fieldPackets, row);
						buffer = binRowPacket.write(buffer, source, true);
					}
					index++;
					if(index == end){
						break;
					}
				}
			} else {
				while (iter.hasNext()){
					UnsafeRow row = iter.next();
					if(index >= start){
						row.packetId = ++packetId;
						buffer = row.write(buffer,source,true);
					}
					index++;
					if(index == end){
						break;
					}
				}
			}

			eof[3] = ++packetId;

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("last packet id:" + packetId);
			}
			//huangyiming add  中间过程缓存起来,isMiddleResultDone是确保合并部分执行完成后才会执行secondExecute
			MiddlerResultHandler middlerResultHandler = source.getSession2().getMiddlerResultHandler();
 			if(null != middlerResultHandler){
 				if(buffer.position() > 0){
 					buffer.flip();
 	                byte[] data = new byte[buffer.limit()];
 	                buffer.get(data);
 	                buffer.clear();
 	                //如果该操作只是一个中间过程则把结果存储起来
 					 String str =  ResultSetUtil.getColumnValAsString(data, fields, 0);
 					 //真的需要数据合并的时候才合并
 					 if(rrs.isHasAggrColumn()){
 						 middlerResultHandler.getResult().clear();
 						 if(str !=null){
  							 middlerResultHandler.add(str);
 						 }
 					 }
 				}
				isMiddleResultDone.set(false);
		}else{
			ByteBuffer byteBuffer = source.writeToBuffer(eof, buffer);

			/**
			 * 真正的开始把Writer Buffer的数据写入到channel 中
			 */
			if(source.canResponse()) {
				source.write(byteBuffer);
			}
			
		}


 		} catch (Exception e) {
			e.printStackTrace();
			handleDataProcessException(e);
		} finally {
			lock.unlock();
			dataMergeSvr.clear();
		}
	}
	public void outputMergeResult(final ServerConnection source,
			final byte[] eof, List<RowDataPacket> results) {
		try {
			lock.lock();
			ByteBuffer buffer = session.getSource().allocate();
			final RouteResultset rrs = this.dataMergeSvr.getRrs();

			// 处理limit语句
			int start = rrs.getLimitStart();
			int end = start + rrs.getLimitSize();

			if (start < 0) {
				start = 0;
			}

			if (rrs.getLimitSize() < 0) {
				end = results.size();
			}

//			// 对于不需要排序的语句,返回的数据只有rrs.getLimitSize()
//			if (rrs.getOrderByCols() == null) {
//				end = results.size();
//				start = 0;
//			}
			if (end > results.size()) {
				end = results.size();
			}

//			for (int i = start; i < end; i++) {
//				RowDataPacket row = results.get(i);
//				if( prepared ) {
//					BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
//					binRowDataPk.read(fieldPackets, row);
//					binRowDataPk.packetId = ++packetId;
//					//binRowDataPk.write(source);
//					buffer = binRowDataPk.write(buffer, session.getSource(), true);
//				} else {
//					row.packetId = ++packetId;
//					buffer = row.write(buffer, source, true);
//				}
//			}

			if(prepared) {
				for (int i = start; i < end; i++) {
					RowDataPacket row = results.get(i);
					BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
					binRowDataPk.read(fieldPackets, row);
					binRowDataPk.packetId = ++packetId;
					//binRowDataPk.write(source);
					buffer = binRowDataPk.write(buffer, session.getSource(), true);
				}
			} else {
				for (int i = start; i < end; i++) {
					RowDataPacket row = results.get(i);
					row.packetId = ++packetId;
					buffer = row.write(buffer, source, true);
				}
			}

			eof[3] = ++packetId;
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("last packet id:" + packetId);
			}
			if(source.canResponse()) {
				source.write(source.writeToBuffer(eof, buffer));
			}

		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
			dataMergeSvr.clear();
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {
		
		//10个连接有一个连接错误怎么办哦。
		if (errorRepsponsed.get()|| this.isFail()) {
			// the connection has been closed or set to "txInterrupt" properly
			//in tryErrorFinished() method! If we close it here, it can
			// lead to tx error such as blocking rollback tx for ever.
			// @author Uncle-pan
			// @since 2016-03-25
			// conn.close(this.error);
			return;
		}
		
		//huangyiming add
		this.header = header;
		this.fields = fields;
		MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
        /*if(null !=middlerResultHandler ){
			return;
		}*/
		this.netOutBytes += header.length;
		this.netOutBytes += eof.length;
		for (int i = 0, len = fields.size(); i < len; ++i) {
			byte[] field = fields.get(i);
			this.netOutBytes += field.length;
		}

		ServerConnection source = null;

		if (fieldsReturned) {
			return;
		}
		lock.lock();
		try {
			if (fieldsReturned) {
				return;
			}
			fieldsReturned = true;

			boolean needMerg = (dataMergeSvr != null)
					&& dataMergeSvr.getRrs().needMerge();
			Set<String> shouldRemoveAvgField = new HashSet<>();
			Set<String> shouldRenameAvgField = new HashSet<>();
			if (needMerg) {
				Map<String, Integer> mergeColsMap = dataMergeSvr.getRrs()
						.getMergeCols();
				if (mergeColsMap != null) {
					for (Map.Entry<String, Integer> entry : mergeColsMap
							.entrySet()) {
						String key = entry.getKey();
						int mergeType = entry.getValue();
						if (MergeCol.MERGE_AVG == mergeType
								&& mergeColsMap.containsKey(key + "SUM")) {
							shouldRemoveAvgField.add((key + "COUNT")
									.toUpperCase());
							shouldRenameAvgField.add((key + "SUM")
									.toUpperCase());
						}
					}
				}

			}

			source = session.getSource();
			ByteBuffer buffer = source.allocate();
			fieldCount = fields.size();
			if (shouldRemoveAvgField.size() > 0) {
				ResultSetHeaderPacket packet = new ResultSetHeaderPacket();
				packet.packetId = ++packetId;
				packet.fieldCount = fieldCount - shouldRemoveAvgField.size();
				buffer = packet.write(buffer, source, true);
			} else {

				header[3] = ++packetId;
				buffer = source.writeToBuffer(header, buffer);
			}

			String primaryKey = null;
			if (rrs.hasPrimaryKeyToCache()) {
				String[] items = rrs.getPrimaryKeyItems();
				priamaryKeyTable = items[0];
				primaryKey = items[1];
			}

			Map<String, ColMeta> columToIndx = new HashMap<String, ColMeta>(
					fieldCount);

			for (int i = 0, len = fieldCount; i < len; ++i) {
				boolean shouldSkip = false;
				byte[] field = fields.get(i);
				if (needMerg) {
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					fieldPackets.add(fieldPkg);
					String fieldName = new String(fieldPkg.name).toUpperCase();
					if (columToIndx != null
							&& !columToIndx.containsKey(fieldName)) {
						if (shouldRemoveAvgField.contains(fieldName)) {
							shouldSkip = true;
							fieldPackets.remove(fieldPackets.size() - 1);
						}
						if (shouldRenameAvgField.contains(fieldName)) {
							String newFieldName = fieldName.substring(0,
									fieldName.length() - 3);
							fieldPkg.name = newFieldName.getBytes();
							fieldPkg.packetId = ++packetId;
							shouldSkip = true;
							// 处理AVG字段位数和精度, AVG位数 = SUM位数 - 14
							fieldPkg.length = fieldPkg.length - 14;
							// AVG精度 = SUM精度 + 4
 							fieldPkg.decimals = (byte) (fieldPkg.decimals + 4);
							buffer = fieldPkg.write(buffer, source, false);

							// 还原精度
							fieldPkg.decimals = (byte) (fieldPkg.decimals - 4);
						}

						ColMeta colMeta = new ColMeta(i, fieldPkg.type);
						colMeta.decimals = fieldPkg.decimals;
						columToIndx.put(fieldName, colMeta);
					}
				} else {
					FieldPacket fieldPkg = new FieldPacket();
					fieldPkg.read(field);
					fieldPackets.add(fieldPkg);
					fieldCount = fields.size();
					if (primaryKey != null && primaryKeyIndex == -1) {
					// find primary key index
					String fieldName = new String(fieldPkg.name);
					if (primaryKey.equalsIgnoreCase(fieldName)) {
						primaryKeyIndex = i;
					}
				}   }
				if (!shouldSkip) {
					field[3] = ++packetId;
					buffer = source.writeToBuffer(field, buffer);
				}
			}
			eof[3] = ++packetId;
			buffer = source.writeToBuffer(eof, buffer);

			if(null == middlerResultHandler ){
				//session.getSource().write(row);
				source.write(buffer);
		     }

 			if (dataMergeSvr != null) {
				dataMergeSvr.onRowMetaData(columToIndx, fieldCount);

			}
		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
		}
	}

	public void handleDataProcessException(Exception e) {
		if (!errorRepsponsed.get()) {
			this.error = e.toString();
			LOGGER.warn(this.toString() +" caught exception ", e);
			setFail(e.toString());
			//判断是否全部返回
			boolean finished = false;
			lock.lock();
			try {
				finished = (this.nodeCount == 0);

			} finally {
				lock.unlock();
			}
			this.tryErrorFinished(finished);
		}
	}

	@Override
	public void rowResponse(final byte[] row, final BackendConnection conn) {

 		if (errorRepsponsed.get()||this.isFail()) {
			// the connection has been closed or set to "txInterrupt" properly
			//in tryErrorFinished() method! If we close it here, it can
			// lead to tx error such as blocking rollback tx for ever.
			// @author Uncle-pan
			// @since 2016-03-25
			//conn.close(error);
			return;
		}


		lock.lock();
		try {

			this.selectRows++;

			RouteResultsetNode rNode = (RouteResultsetNode) conn.getAttachment();
			String dataNode = rNode.getName();
			if (dataMergeSvr != null) {
				// even through discarding the all rest data, we can't
				//close the connection for tx control such as rollback or commit.
				// So the "isClosedByDiscard" variable is unnecessary.
				// @author Uncle-pan
				// @since 2016-03-25
					dataMergeSvr.onNewRecord(dataNode, row);

				MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
 				if(null != middlerResultHandler ){
 					 if(middlerResultHandler instanceof MiddlerQueryResultHandler){
 						 byte[] rv = ResultSetUtil.getColumnVal(row, fields, 0);
						 String rowValue =  rv==null? "":new String(rv);
						 middlerResultHandler.add(rowValue);
 					 }
				}
			} else {
				row[3] = ++packetId;
				RowDataPacket rowDataPkg =null;
				// cache primaryKey-> dataNode
				if (primaryKeyIndex != -1) {
					 rowDataPkg = new RowDataPacket(fieldCount);
					rowDataPkg.read(row);
					String primaryKey = new String(rowDataPkg.fieldValues.get(primaryKeyIndex));
					LayerCachePool pool = MycatServer.getInstance().getRouterservice().getTableId2DataNodeCache();
					pool.putIfAbsent(priamaryKeyTable, primaryKey, dataNode);
				}
				if( prepared ) {
					if(rowDataPkg==null) {
						rowDataPkg = new RowDataPacket(fieldCount);
						rowDataPkg.read(row);
					}
					BinaryRowDataPacket binRowDataPk = new BinaryRowDataPacket();
					binRowDataPk.read(fieldPackets, rowDataPkg);
					binRowDataPk.write(session.getSource());
				} else {
					//add huangyiming
					MiddlerResultHandler middlerResultHandler = session.getMiddlerResultHandler();
					if(null == middlerResultHandler ){
 						session.getSource().write(row);
					}else{

						 if(middlerResultHandler instanceof MiddlerQueryResultHandler){
							 String rowValue =  ResultSetUtil.getColumnValAsString(row, fields, 0);
							 middlerResultHandler.add(rowValue);
 						 }

					}
				}
			}

		} catch (Exception e) {
			handleDataProcessException(e);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void clearResources() {
		if (dataMergeSvr != null) {
			dataMergeSvr.clear();
		}
	}

	@Override
	public void writeQueueAvailable() {
	}

	@Override
	public void requestDataResponse(byte[] data, BackendConnection conn) {
		LoadDataUtil.requestFileDataResponse(data, conn);
	}

	public boolean isPrepared() {
		return prepared;
	}

	public void setPrepared(boolean prepared) {
		this.prepared = prepared;
	}
}