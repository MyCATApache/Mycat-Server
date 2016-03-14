package io.mycat.backend.postgresql;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.model.DBHostConfig;
import io.mycat.net.NIOConnector;
import io.mycat.net.factory.BackendConnectionFactory;

public class PostgreSQLBackendConnectionFactory extends BackendConnectionFactory {
	PostgreSQLBackendConnectionHandler nioHandler = new PostgreSQLBackendConnectionHandler();

	public PostgreSQLBackendConnection make(PostgreSQLDataSource pool, ResponseHandler handler, String schema)
			throws IOException {

		DBHostConfig dsc = pool.getConfig();
		SocketChannel channel = SocketChannel.open();
		channel.configureBlocking(false);

		PostgreSQLBackendConnection c = new PostgreSQLBackendConnection(channel, pool.isReadNode());
		MycatServer.getInstance().getConfig().setSocketParams(c, false);
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
		((NIOConnector) MycatServer.getInstance().getConnector()).postConnect(c);
				return c;
	}

}
