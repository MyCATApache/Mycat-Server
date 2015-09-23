package io.mycat.backend.nio;

import io.mycat.backend.MySQLDataSource;
import io.mycat.net.NetSystem;
import io.mycat.server.config.node.DBHostConfig;
import io.mycat.server.executors.ResponseHandler;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class MySQLBackendConnectionFactory {
	private final MySQLBackendConnectionHandler nioHandler = new MySQLBackendConnectionHandler();

	public MySQLBackendConnection make(MySQLDataSource pool,
			ResponseHandler handler, String schema) throws IOException {

		DBHostConfig dsc = pool.getConfig();
		SocketChannel channel = SocketChannel.open();
		channel.configureBlocking(false);

		MySQLBackendConnection c = new MySQLBackendConnection(channel,
				pool.isReadNode());
		NetSystem.getInstance().setSocketParams(c, false);
		// 设置NIOHandler
		c.setHandler(nioHandler);
		c.setHost(dsc.getIp());
		c.setPort(dsc.getPort());
		c.setUser(dsc.getUser());
		c.setPassword(dsc.getPassword());
		c.setSchema(schema);
		c.setPool(pool);
		c.setResponseHandler(handler);
		c.setIdleTimeout(pool.getConfig().getIdleTimeout());
		NetSystem.getInstance().getConnector().postConnect(c);
		return c;
	}
}
