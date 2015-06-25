package io.mycat.mysql;

import io.mycat.MycatPrivileges;
import io.mycat.MycatServer;
import io.mycat.SystemConfig;
import io.mycat.net.nio.Connection;
import io.mycat.net.nio.ConnectionFactory;
import io.mycat.net.nio.NIOHandler;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public class MySQLFrontConnectionFactory extends ConnectionFactory {

	private final NIOHandler<MySQLFrontConnection> nioHandler;

	public MySQLFrontConnectionFactory(
			NIOHandler<MySQLFrontConnection> nioHandler) {
		super();
		this.nioHandler = nioHandler;
	}

	@Override
	protected Connection makeConnection(SocketChannel channel)
			throws IOException {
		MySQLFrontConnection con = new MySQLFrontConnection(channel);
		SystemConfig sys = MycatServer.getInstance().getConfig().getSystem();
		con.setPrivileges(MycatPrivileges.instance());
		con.setCharset(sys.getCharset());
		// con.setLoadDataInfileHandler(new ServerLoadDataInfileHandler(c));
		// c.setPrepareHandler(new ServerPrepareHandler(c));
		con.setTxIsolation(sys.getTxIsolation());
		return con;
	}

	@Override
	protected NIOHandler<MySQLFrontConnection> getNIOHandler() {

		return nioHandler;
	}

}
