package io.mycat.backend.postgresql;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.nio.handler.ResponseHandler;
import io.mycat.config.model.DBHostConfig;
import io.mycat.net.NIOConnector;
import io.mycat.net.factory.BackendConnectionFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NetworkChannel;

public class PostgreSQLBackendConnectionFactory extends
		BackendConnectionFactory {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public PostgreSQLBackendConnection make(PostgreSQLDataSource pool,
			ResponseHandler handler, final String schema) throws IOException {

		final DBHostConfig dsc = pool.getConfig();
		NetworkChannel channel = this.openSocketChannel(MycatServer
				.getInstance().isAIO());

		final PostgreSQLBackendConnection c = new PostgreSQLBackendConnection(
				channel, pool.isReadNode());
		MycatServer.getInstance().getConfig().setSocketParams(c, false);
		// 设置NIOHandler
		c.setHandler(new PostgreSQLBackendConnectionHandler(c));
		c.setHost(dsc.getIp());
		c.setPort(dsc.getPort());
		c.setUser(dsc.getUser());
		c.setPassword(dsc.getPassword());
		c.setSchema(schema);
		c.setPool(pool);
		c.setResponseHandler(handler);
		c.setIdleTimeout(pool.getConfig().getIdleTimeout());
		if (channel instanceof AsynchronousSocketChannel) {
			((AsynchronousSocketChannel) channel).connect(
					new InetSocketAddress(dsc.getIp(), dsc.getPort()), c,
					(CompletionHandler) MycatServer.getInstance()
							.getConnector());
		} else {
			((NIOConnector) MycatServer.getInstance().getConnector())
					.postConnect(c);

		}
//		SocketChannel chan = (SocketChannel)c.getChannel();
//		chan.write(PacketUtils.makeStartUpPacket(dsc.getUser(), schema));
//		
		//  发送启动包。		
//		new java.lang.Thread(new Runnable() {
//			
//			@Override
//			public void run() {
//				try {
//					Thread.sleep(100);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//				try {
//					c.write(PacketUtils.makeStartUpPacket(dsc.getUser(), schema));
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}).start();
		
		return c;
	}
	
	
}
