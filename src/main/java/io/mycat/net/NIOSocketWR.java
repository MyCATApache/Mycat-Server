package io.mycat.net;

import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NIO socket的读写
 */
public class NIOSocketWR extends SocketWR {
	private SelectionKey processKey;
	private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
	private final AbstractConnection con;
	private final SocketChannel channel;
	private final AtomicBoolean writing = new AtomicBoolean(false);
	protected static final Logger LOGGER = LoggerFactory.getLogger(NIOSocketWR.class);
	public static final ByteBuffer EMPTY_BYTEBUFFER  = ByteBuffer.allocate(1);
	public NIOSocketWR(AbstractConnection con) {
		this.con = con;
		this.channel = (SocketChannel) con.channel;
	}

	/**
	 * 将channel注册到选择器上。
	 * @param selector 选择器
	 * @throws IOException
	 */
	public void register(Selector selector) throws IOException {
		try {
			// 使用给定的选择器注册此通道，返回选择键。
			processKey = channel.register(selector, SelectionKey.OP_READ, con);
		} finally {
			if (con.isClosed.get()) {
				clearSelectionKey();
			}
		}
	}

	/**
	 * 执行下一个写检查
	 */
	@Override
	public void doNextWriteCheck() {
		if (!writing.compareAndSet(false, true)) {
			return;
		}

		try {
			if(!channel.isOpen()){
				AbstractConnection.LOGGER.debug("caught err: {}", con);
			}
			boolean noMoreData = write0();
			writing.set(false);
			if (noMoreData && con.writeQueue.isEmpty()) {
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
					// 关闭SelectionKey的写操作
					disableWrite();
				}
			} else {
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
					// 开启SelectionKey的写操作
					enableWrite(false);
				}
			}
		} catch (IOException e) {
			if (AbstractConnection.LOGGER.isDebugEnabled()) {
				AbstractConnection.LOGGER.debug("caught err:", e);
			}
			con.close("err:" + e);
		} finally {
			writing.set(false);
		}
	}

	@Override
	public boolean checkAlive() {
		try {
			return 	channel.read(EMPTY_BYTEBUFFER) == 0;
		} catch (IOException e) {
			LOGGER.error("",e);
			return false;
		}finally {
			EMPTY_BYTEBUFFER.position(0);
		}
	}

	/**
	 * 往channel写数据，即回复
	 * @return true表示写完数据 false表示有数据未写完
	 * @throws IOException
	 */
	private boolean write0() throws IOException {
		int written = 0;
		ByteBuffer buffer = con.writeBuffer;
		if (buffer != null) {
			while (buffer.hasRemaining()) {
				// 有数据
				written = channel.write(buffer); // 管理器的管理请求 或 服务器的SQL请求 的回复
				if (written > 0) {
					con.netOutBytes += written;
					con.processor.addNetOutBytes(written);
					con.lastWriteTime = TimeUtil.currentTimeMillis();
				} else {
					break;
				}
			}

			if (buffer.hasRemaining()) {
				// 还有数据
				con.writeAttempts++;
				return false;
			} else {
				con.writeBuffer = null;
				con.recycle(buffer);
			}
		}
		while ((buffer = con.writeQueue.poll()) != null) {
			if (buffer.limit() == 0) {
				con.recycle(buffer);
				con.close("quit send");
				return true;
			}

			buffer.flip();
			try {
				while (buffer.hasRemaining()) {
					// 有数据
					written = channel.write(buffer); // 管理器的管理请求 或 服务器的SQL请求 的回复 // TODO java.io.IOException: Connection reset by peer
					if (written > 0) {
						con.lastWriteTime = TimeUtil.currentTimeMillis();
						con.netOutBytes += written;
						con.processor.addNetOutBytes(written);
						con.lastWriteTime = TimeUtil.currentTimeMillis();
					} else {
						break;
					}
				}
			} catch (IOException e) {
				con.recycle(buffer);
				throw e;
			}
			if (buffer.hasRemaining()) {
				// 有数据
				con.writeBuffer = buffer;
				con.writeAttempts++;
				return false;
			} else {
				con.recycle(buffer);
			}
		}
		return true;
	}

	/**
	 * 关闭SelectionKey的写操作
	 */
	private void disableWrite() {
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() & OP_NOT_WRITE);
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("can't disable write " + e + " con " + con);
		}
	}

	/**
	 * 开启SelectionKey的写操作
	 * @param wakeup 是否唤醒选择器
	 */
	private void enableWrite(boolean wakeup) {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			needWakeup = true;
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("can't enable write " + e);
		}
		if (needWakeup && wakeup) {
			processKey.selector().wakeup();
		}
	}

	public void disableRead() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_READ);
	}

	public void enableRead() {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_READ);
			needWakeup = true;
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("enable read fail " + e);
		}
		if (needWakeup) {
			processKey.selector().wakeup();
		}
	}

	private void clearSelectionKey() {
		try {
			SelectionKey key = this.processKey;
			if (key != null && key.isValid()) {
				key.attach(null);
				key.cancel();
			}
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("clear selector keys err:" + e);
		}
	}

	/**
	 * 异步读
	 */
	@Override
	public void asynRead() throws IOException {
		ByteBuffer theBuffer = con.readBuffer;
		if (theBuffer == null) {
			theBuffer = con.processor.getBufferPool().allocate(con.processor.getBufferPool().getChunkSize());
			con.readBuffer = theBuffer;
		}
		// 将channel的数据读到字节缓存中
		int got = channel.read(theBuffer);
		con.onReadData(got);
	}

}
