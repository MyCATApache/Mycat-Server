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
package org.opencloudb.server;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.MycatConfig;
import org.opencloudb.MycatServer;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.backend.PhysicalDBNode;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.model.SystemConfig;
import org.opencloudb.mysql.nio.handler.CommitNodeHandler;
import org.opencloudb.mysql.nio.handler.KillConnectionHandler;
import org.opencloudb.mysql.nio.handler.MultiNodeCoordinator;
import org.opencloudb.mysql.nio.handler.MultiNodeQueryHandler;
import org.opencloudb.mysql.nio.handler.RollbackNodeHandler;
import org.opencloudb.mysql.nio.handler.RollbackReleaseHandler;
import org.opencloudb.mysql.nio.handler.SingleNodeHandler;
import org.opencloudb.net.FrontendConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.sqlcmd.SQLCmdConstant;

/**
 * @author mycat
 * @author mycat
 */
public class NonBlockingSession implements Session {
	public static final Logger LOGGER = Logger
			.getLogger(NonBlockingSession.class);

	private final ServerConnection source;
	private final ConcurrentHashMap<RouteResultsetNode, BackendConnection> target;
	// life-cycle: each sql execution
	private volatile SingleNodeHandler singleNodeHandler;
	private volatile MultiNodeQueryHandler multiNodeHandler;
	private volatile RollbackNodeHandler rollbackHandler;
	private final MultiNodeCoordinator multiNodeCoordinator;
	private final CommitNodeHandler commitHandler;
	private volatile String xaTXID;

	public NonBlockingSession(ServerConnection source) {
		this.source = source;
		this.target = new ConcurrentHashMap<RouteResultsetNode, BackendConnection>(
				2, 0.75f);
		multiNodeCoordinator = new MultiNodeCoordinator(this);
		commitHandler = new CommitNodeHandler(this);
	}

	@Override
	public ServerConnection getSource() {
		return source;
	}

	@Override
	public int getTargetCount() {
		return target.size();
	}

	public Set<RouteResultsetNode> getTargetKeys() {
		return target.keySet();
	}

	public BackendConnection getTarget(RouteResultsetNode key) {
		return target.get(key);
	}

	public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
		return this.target;
	}

	public BackendConnection removeTarget(RouteResultsetNode key) {
		return target.remove(key);
	}

	@Override
	public void execute(RouteResultset rrs, int type) {
		// clear prev execute resources
		clearHandlesResources();
		if (LOGGER.isDebugEnabled()) {
			StringBuilder s = new StringBuilder();
			LOGGER.debug(s.append(source).append(rrs).toString() + " rrs ");
		}

		// 检查路由结果是否为空
		RouteResultsetNode[] nodes = rrs.getNodes();
		if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
				|| nodes[0].getName().equals("")) {
			source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
					"No dataNode found ,please check tables defined in schema:"
							+ source.getSchema());
			return;
		}
		if (nodes.length == 1) {
			singleNodeHandler = new SingleNodeHandler(rrs, this);
			try {
				singleNodeHandler.execute();
			} catch (Exception e) {
				LOGGER.warn(new StringBuilder().append(source).append(rrs), e);
				source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
			}
		} else {
			boolean autocommit = source.isAutocommit();
			SystemConfig sysConfig = MycatServer.getInstance().getConfig()
					.getSystem();
			int mutiNodeLimitType = sysConfig.getMutiNodeLimitType();
			multiNodeHandler = new MultiNodeQueryHandler(type, rrs, autocommit,
					this);

			try {
				multiNodeHandler.execute();
			} catch (Exception e) {
				LOGGER.warn(new StringBuilder().append(source).append(rrs), e);
				source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
			}
		}
	}

	public void commit() {
		final int initCount = target.size();
		if (initCount <= 0) {
			ByteBuffer buffer = source.allocate();
			buffer = source.writeToBuffer(OkPacket.OK, buffer);
			source.write(buffer);
			return;
		} else if (initCount == 1) {
			BackendConnection con = target.elements().nextElement();
			commitHandler.commit(con);

		} else {

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("multi node commit to send ,total " + initCount);
			}
			multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
		}

	}

	public void rollback() {
		final int initCount = target.size();
		if (initCount <= 0) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("no session bound connections found ,no need send rollback cmd ");
			}
			ByteBuffer buffer = source.allocate();
			buffer = source.writeToBuffer(OkPacket.OK, buffer);
			source.write(buffer);
			return;
		}
		rollbackHandler = new RollbackNodeHandler(this);
		rollbackHandler.rollback();
	}

	@Override
	public void cancel(FrontendConnection sponsor) {

	}

	/**
	 * {@link ServerConnection#isClosed()} must be true before invoking this
	 */
	public void terminate() {
		for (BackendConnection node : target.values()) {
			node.close("client closed ");
		}
		target.clear();
		clearHandlesResources();
	}

	public void closeAndClearResources(String reason) {
		for (BackendConnection node : target.values()) {
			node.close(reason);
		}
		target.clear();
		clearHandlesResources();
	}

	public void releaseConnectionIfSafe(BackendConnection conn, boolean debug,
			boolean needRollback) {
		RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();

		if (node != null) {
			if (this.source.isAutocommit() || conn.isFromSlaveDB()
					|| !conn.isModifiedSQLExecuted()) {
				releaseConnection((RouteResultsetNode) conn.getAttachment(),
						LOGGER.isDebugEnabled(), needRollback);
			}
		}
	}

	public void releaseConnection(RouteResultsetNode rrn, boolean debug,
			final boolean needRollback) {

		BackendConnection c = target.remove(rrn);
		if (c != null) {
			if (debug) {
				LOGGER.debug("release connection " + c);
			}
			if (c.getAttachment() != null) {
				c.setAttachment(null);
			}
			if (!c.isClosedOrQuit()) {
				if (c.isAutocommit()) {
					c.release();
				} else
              //  if (needRollback)
              {
					c.setResponseHandler(new RollbackReleaseHandler());
					c.rollback();
			}
  //              else {
//					c.release();
//				}
			}
		}
	}

	public void releaseConnections(final boolean needRollback) {
		boolean debug = LOGGER.isDebugEnabled();
		for (RouteResultsetNode rrn : target.keySet()) {
			releaseConnection(rrn, debug, needRollback);
		}
	}

	public void releaseConnection(BackendConnection con) {
		Iterator<Entry<RouteResultsetNode, BackendConnection>> itor = target
				.entrySet().iterator();
		while (itor.hasNext()) {
			BackendConnection theCon = itor.next().getValue();
			if (theCon == con) {
				itor.remove();
				con.release();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("realse connection " + con);
				}
				break;
			}
		}

	}

	/**
	 * @return previous bound connection
	 */
	public BackendConnection bindConnection(RouteResultsetNode key,
			BackendConnection conn) {
		// System.out.println("bind connection "+conn+
		// " to key "+key.getName()+" on sesion "+this);
		return target.put(key, conn);
	}

	public boolean tryExistsCon(final BackendConnection conn,
			RouteResultsetNode node) {

		if (conn == null) {
			return false;
		}
		//检查是否为Slave读节点并且有效
		if (!conn.isFromSlaveDB()
				|| node.canRunnINReadDB(getSource().isAutocommit())) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("found connections in session to use " + conn
						+ " for " + node);
			}

			conn.setAttachment(node);
			return true;
		} else {
			// slavedb connection and can't use anymore ,release it
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("release slave connection,can't be used in trasaction  "
						+ conn + " for " + node);
			}
			releaseConnection(node, LOGGER.isDebugEnabled(), false);
		}
		return false;
	}

	protected void kill() {
		boolean hooked = false;
		AtomicInteger count = null;
		Map<RouteResultsetNode, BackendConnection> killees = null;
		for (RouteResultsetNode node : target.keySet()) {
			BackendConnection c = target.get(node);
			if (c != null) {
				if (!hooked) {
					hooked = true;
					killees = new HashMap<RouteResultsetNode, BackendConnection>();
					count = new AtomicInteger(0);
				}
				killees.put(node, c);
				count.incrementAndGet();
			}
		}
		if (hooked) {
			for (Entry<RouteResultsetNode, BackendConnection> en : killees
					.entrySet()) {
				KillConnectionHandler kill = new KillConnectionHandler(
						en.getValue(), this);
				MycatConfig conf = MycatServer.getInstance().getConfig();
				PhysicalDBNode dn = conf.getDataNodes().get(
						en.getKey().getName());
				try {
					dn.getConnectionFromSameSource(null,true, en.getValue(),
							kill, en.getKey());
				} catch (Exception e) {
					LOGGER.error(
							"get killer connection failed for " + en.getKey(),
							e);
					kill.connectionError(e, null);
				}
			}
		}
	}

	private void clearHandlesResources() {
		SingleNodeHandler singleHander = singleNodeHandler;
		if (singleHander != null) {
			singleHander.clearResources();
			singleNodeHandler = null;
		}
		MultiNodeQueryHandler multiHandler = multiNodeHandler;
		if (multiHandler != null) {
			multiHandler.clearResources();
			multiNodeHandler = null;
		}
	}

	public void clearResources(final boolean needRollback) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("clear session resources " + this);
		}
		this.releaseConnections(needRollback);
		clearHandlesResources();
	}

	public boolean closed() {
		return source.isClosed();
	}

	private String genXATXID() {
		return MycatServer.getInstance().genXATXID();
	}

	public void setXATXEnabled(boolean xaTXEnabled) {

		LOGGER.info("XA Transaction enabled ,con " + this.getSource());
		if (xaTXEnabled && this.xaTXID == null) {
			xaTXID = genXATXID();

		}

	}

	public String getXaTXID() {
		return xaTXID;
	}

}