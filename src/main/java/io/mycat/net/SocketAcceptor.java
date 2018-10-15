package io.mycat.net;

/**
 * socket接收器
 */
public interface SocketAcceptor {

	void start();

	String getName();

	int getPort();

}
