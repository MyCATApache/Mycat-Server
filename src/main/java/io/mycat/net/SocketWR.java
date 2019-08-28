package io.mycat.net;

import java.io.IOException;

/**
 * socket的读写
 */
public abstract class SocketWR {
	/**
	 * 异步读
	 * @throws IOException
	 */
	public abstract void asynRead() throws IOException;

	/**
	 * 执行下一个写检查
	 */
	public abstract void doNextWriteCheck() ;
	public abstract boolean checkAlive();
}
