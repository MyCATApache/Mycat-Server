package io.mycat.backend.mysql.nio.handler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.backend.BackendConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;
import io.mycat.server.parser.ServerParse;

/**
 * unlock tables 语句处理器
 * @author songdabin
 *
 */
public class UnLockTablesHandler extends MultiNodeHandler implements ResponseHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(UnLockTablesHandler.class);

	private final NonBlockingSession session;
	private final boolean autocommit;
	private final String srcStatement;

	public UnLockTablesHandler(NonBlockingSession session, boolean autocommit, String sql) {
		super(session);
		this.session = session;
		this.autocommit = autocommit;
		this.srcStatement = sql;
	}

	public void execute() {
		ConcurrentHashMap<RouteResultsetNode, BackendConnection> lockedConns = session.getLockedTargetMap();
		Set<RouteResultsetNode> dnSet = lockedConns.keySet();
		this.reset(lockedConns.size());
		// 客户端直接发送unlock tables命令，由于之前未发送lock tables语句，无法获取后端绑定的连接，此时直接返回OK包
		if (lockedConns.size() == 0) {
			LOGGER.warn("find no locked backend connection!"+session.getSource());
			OkPacket ok = new OkPacket();
			ok.packetId = ++ packetId;
			ok.packetLength = 7; // unlock table 命令返回MySQL协议包长度为7
			ok.serverStatus = session.getSource().isAutocommit() ? 2:1;
			ok.write(session.getSource());
			return;
		}
		for (RouteResultsetNode dataNode : dnSet) {
			RouteResultsetNode node = new RouteResultsetNode(dataNode.getName(), ServerParse.UNLOCK, srcStatement);
			BackendConnection conn = lockedConns.get(dataNode);
			if (clearIfSessionClosed(session)) {
				return;
			}
			conn.setResponseHandler(this);
			try {
				conn.execute(node, session.getSource(), autocommit);
			} catch (Exception e) {
				connectionError(e, conn);
			}
		}
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
		super.connectionError(e, conn);
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {
		LOGGER.error("unexpected invocation: connectionAcquired from unlock tables");
	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		super.errorResponse(err, conn);
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			boolean isEndPack = decrementCountBy(1);
			session.releaseLockedConnection(conn);
			session.releaseConnection(conn);
			if (isEndPack) {
				if (this.isFail() || session.closed()) {
					tryErrorFinished(true);
					return;
				}
				OkPacket ok = new OkPacket();
				ok.read(data);
				lock.lock();
				try {
					ok.packetId = ++ packetId;
					ok.serverStatus = session.getSource().isAutocommit() ? 2:1;
				} finally {
					lock.unlock();
				}
				ok.write(session.getSource());
			}
		}
	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields, byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": field's eof").toString());
	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {
		LOGGER.warn(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": row data packet").toString());
	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
		LOGGER.error(new StringBuilder().append("unexpected packet for ")
				.append(conn).append(" bound by ").append(session.getSource())
				.append(": row's eof").toString());
	}

	@Override
	public void writeQueueAvailable() {
		// TODO Auto-generated method stub

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {
		// TODO Auto-generated method stub

	}

}
