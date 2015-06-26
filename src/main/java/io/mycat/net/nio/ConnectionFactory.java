package io.mycat.net.nio;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wuzh
 */
public abstract class ConnectionFactory {

	/**
	 * 创建一个具体的连接
	 * 
	 * @param channel
	 * @return Connection
	 * @throws IOException
	 */
	protected abstract Connection makeConnection(SocketChannel channel)
			throws IOException;

	/**
	 * NIOHandler是无状态的，多个连接共享一个，因此建议作为 Factory的私有变量
	 * 
	 * @return NIOHandler
	 */
	@SuppressWarnings("rawtypes")
	protected abstract NIOHandler getNIOHandler();

	@SuppressWarnings("unchecked")
	public Connection make(SocketChannel channel) throws IOException {
		channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		// 子类完成具体连接创建工作
		Connection c = makeConnection(channel);
		// 设置连接的参数
		NetSystem.getInstance().setSocketParams(c,true);
		// 设置NIOHandler
		c.setHandler(getNIOHandler());
		return c;
	}
}

@SuppressWarnings("rawtypes")
class NIOHandlerWrap implements NIOHandler {
	protected static final Logger LOGGER = LoggerFactory
			.getLogger(NIOHandlerWrap.class);
	private final NIOHandler handler;

	public NIOHandlerWrap(NIOHandler handler) {
		super();
		this.handler = handler;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onConnected(Connection con) throws IOException {
		con.setState(Connection.State.connecting);
		String info = con.getDirection() == Connection.Direction.in ? "remote peer connected to me "
				+ con
				: " connected to remote peer " + con;
		LOGGER.info(info);
		handler.onConnected(con);

	}

	@SuppressWarnings("unchecked")
	@Override
	public void onConnectFailed(Connection con, Throwable e) {
		LOGGER.warn("connection failed: " + e + " con " + con);
		handler.onConnectFailed(con, e);

	}

	@SuppressWarnings("unchecked")
	@Override
	public void handle(Connection con, ByteBuffer data, int start,
			int readeLength) {
		handler.handle(con, data, start, readeLength);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onClosed(Connection con, String reason) {
		handler.onClosed(con, reason);

	}

}