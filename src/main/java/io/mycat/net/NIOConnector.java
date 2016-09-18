package io.mycat.net;

import io.mycat.backend.postgresql.PostgreSQLBackendConnection;
import io.mycat.backend.postgresql.utils.PacketUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * NIO 连接器，用于连接对方Sever
 * 
 * @author wuzh
 */
public final class NIOConnector extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
	

	private final String name;
	private final Selector selector;
	private final BlockingQueue<Connection> connectQueue;
	private long connectCount;
	private final NIOReactorPool reactorPool;

	public NIOConnector(String name, NIOReactorPool reactorPool)
			throws IOException {
		super.setName(name);
		this.name = name;
		this.selector = Selector.open();
		this.reactorPool = reactorPool;
		this.connectQueue = new LinkedBlockingQueue<Connection>();
	}

	public long getConnectCount() {
		return connectCount;
	}

	/**
	 * 添加一个需要异步连接的Connection到队列中，等待连接
	 * 
	 * @param Connection
	 */
	public void postConnect(Connection c) {
		connectQueue.offer(c);
		selector.wakeup();
	}

	@Override
	public void run() {
		final Selector selector = this.selector;
		for (;;) {
			++connectCount;
			try {
				selector.select(1000L);
				connect(selector);
				Set<SelectionKey> keys = selector.selectedKeys();
				try {
					for (SelectionKey key : keys) {
						Object att = key.attachment();
						if (att != null && key.isValid() && key.isConnectable()) {
							finishConnect(key, att);
							if (att instanceof  PostgreSQLBackendConnection){//ONLY PG SENG
								SocketChannel sc = (SocketChannel) key.channel();
								sendStartupPacket(sc,att);
							}
						} else {
							key.cancel();
						}
					}
				} finally {
					keys.clear();
				}
			} catch (Throwable e) {
				LOGGER.warn(name, e);
			}
		}
	}

	//TODO COOLLF  暂时为权宜之计,后续要进行代码结构封调整.
	private static void sendStartupPacket(SocketChannel socketChannel, Object _att) throws IOException {
		PostgreSQLBackendConnection att = (PostgreSQLBackendConnection) _att;
		ByteBuffer buffer = PacketUtils.makeStartUpPacket(att.getUser(), att.getSchema());
		buffer.flip();
		socketChannel.write(buffer);
	}


	private void connect(Selector selector) {
		Connection c = null;
		while ((c = connectQueue.poll()) != null) {
			try {
				SocketChannel channel = (SocketChannel) c.getChannel();
				channel.register(selector, SelectionKey.OP_CONNECT, c);
				channel.connect(new InetSocketAddress(c.host, c.port));
			} catch (Throwable e) {
				c.close("connect failed:" + e.toString());
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void finishConnect(SelectionKey key, Object att) {
		Connection c = (Connection) att;
		try {
			if (finishConnect(c, (SocketChannel) c.channel)) {
				clearSelectionKey(key);
				c.setId(ConnectIdGenerator.getINSTNCE().getId());
				System.out.println("----------------ConnectIdGenerator.getINSTNCE().getId()-----------------"+ConnectIdGenerator.getINSTNCE().getId());
				NIOReactor reactor = reactorPool.getNextReactor();
				reactor.postRegister(c);

			}
		} catch (Throwable e) {
			clearSelectionKey(key);
			c.close(e.toString());
			c.getHandler().onConnectFailed(c, e);

		}
	}

	private boolean finishConnect(Connection c, SocketChannel channel)
			throws IOException {
		System.out.println("----------------finishConnect-----------------");
		if (channel.isConnectionPending()) {
			System.out.println("----------------finishConnect-isConnectionPending-----------------");
			channel.finishConnect();
			// c.setLocalPort(channel.socket().getLocalPort());
			return true;
		} else {
			return false;
		}
	}

	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}

	

}