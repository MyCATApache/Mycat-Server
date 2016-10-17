package io.mycat.backend.mysql.nio.handler;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.config.MycatConfig;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

/**
 * lock tables 语句处理器
 * @author songdabin
 * 
 */
public class LockTablesHandler extends MultiNodeHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(LockTablesHandler.class);

	private final RouteResultset rrs;
	private final ReentrantLock lock;
	private final boolean autocommit;
	
	public LockTablesHandler(NonBlockingSession session, RouteResultset rrs) {
		super(session);
		this.rrs = rrs;
		this.autocommit = session.getSource().isAutocommit();
		this.lock = new ReentrantLock();
	}
	
	public void execute() throws Exception {
		super.reset(this.rrs.getNodes().length);
		MycatConfig conf = MycatServer.getInstance().getConfig();
		for (final RouteResultsetNode node : rrs.getNodes()) {
			BackendConnection conn = session.getTarget(node);
			if (session.tryExistsCon(conn, node)) {
				// 若该连接已被绑定到session.target中，则继续添加到session.lockTarget中
				session.bindLockTableConnection(node, conn);
				_execute(conn, node);
			} else {
				// create new connection
				PhysicalDBNode dn = conf.getDataNodes().get(node.getName());
				dn.getConnection(dn.getDatabase(), autocommit, node, this, node);
			}

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
	public void connectionAcquired(BackendConnection conn) {
		final RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();
		// 将新获取的后端连接分别绑定到session.target和session.lockTarget
		session.bindLockTableConnection(node, conn);
		session.bindConnection(node, conn);
		LOGGER.info("bind lock table connection:"+node.getName()+"->"+conn.toString());
		_execute(conn, node);
	}

	@Override
	public void okResponse(byte[] data, BackendConnection conn) {
		boolean executeResponse = conn.syncAndExcute();
		if (executeResponse) {
			if (clearIfSessionClosed(session)) {
                return;
            }
			boolean isEndPack = decrementCountBy(1);
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
	
	protected String byte2Str(byte[] data) {
		StringBuilder sb = new StringBuilder();
		for (byte b : data) {
			sb.append(Byte.toString(b));
		}
		return sb.toString();
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

}
