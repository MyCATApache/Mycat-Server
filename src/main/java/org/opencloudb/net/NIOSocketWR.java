package org.opencloudb.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opencloudb.util.TimeUtil;


/**
 * 业务线程先通过加锁或者AtomicBoolean判断当前channel是否正在写数据，
 * 如空闲则由当前线程直接写，否则入缓冲队列交给其他线程写；在写的时候，
 * 网络慢等原因导致未写完， 然后注册写事件，由NIOReactor线程(selector)进行候补写
 */
public class NIOSocketWR extends SocketWR {
	
	private SelectionKey processKey;
	
	private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
	
	private final AbstractConnection con;
	private final SocketChannel channel;
	private final AtomicBoolean writing = new AtomicBoolean(false);

	public NIOSocketWR(AbstractConnection con) {
		this.con = con;
		this.channel = (SocketChannel) con.channel;
	}

	
	public void register(Selector selector) throws IOException {
		try {
			processKey = channel.register(selector, SelectionKey.OP_READ, con);
		} finally {
			if (con.isClosed.get()) {
				clearSelectionKey();
			}
		}
	}

	/**
	 * 1、selector循环写事件侦听
	 * 2、其它业务线程触发的写操作
	 */
	public void doNextWriteCheck() {

		//先判断是否正在写，如果正在写，退出
		if ( !writing.compareAndSet(false, true) ) {
			return;
		}

		try {
			
			//写数据
			boolean noMoreData = write0();

			writing.set(false);
			
			/**
			 * 注：
			 * writing.set(false)必须要在boolean noMoreData = write0()之后和if (noMoreData && con.writeQueue.isEmpty())之前，
			 * 否则会导致当网络流量较低时，消息包缓存在内存中迟迟发不出去的现象。
			 */
			
			if (noMoreData && con.writeQueue.isEmpty()) {
				
				//没有完全写入并且缓冲队列为空,取消注册写事件
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
					disableWrite();
				}

			} else {

				//没有完全写入或者缓冲队列有代写对象,继续注册写事件，候补写
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
					enableWrite(false);
				}
			}

		} catch (IOException e) {
			if (AbstractConnection.LOGGER.isDebugEnabled()) {
				AbstractConnection.LOGGER.debug("caught err:", e);
			}
			con.close("err:" + e);
		}

	}

	/**
	 * write0()方法是只要buffer中还有，就不停写入；
	 * 直到写完所有buffer，或者写入时，返回写入字节为零，表示网络繁忙，就回临时退出写操作
	 */
	private boolean write0() throws IOException {

		int written = 0;
		ByteBuffer buffer = con.writeBuffer;
		if (buffer != null) {
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				if (written > 0) {
					con.netOutBytes += written;
					con.processor.addNetOutBytes(written);
					con.lastWriteTime = TimeUtil.currentTimeMillis();
				} else {
					break;
				}
			}

			if (buffer.hasRemaining()) {
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
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				if (written > 0) {
					con.lastWriteTime = TimeUtil.currentTimeMillis();
					con.netOutBytes += written;
					con.processor.addNetOutBytes(written);
					con.lastWriteTime = TimeUtil.currentTimeMillis();
				} else {
					break;
				}
			}
			if (buffer.hasRemaining()) {
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
	 * 关闭写事件
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
	 * 打开写事件
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

	/**
	 * 关闭读事件
	 */
	public void disableRead() {
		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_READ);
	}

	/**
	 * 打开读事件
	 */
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

	@Override
	public void asynRead() throws IOException {
		
		ByteBuffer theBuffer = con.readBuffer;
		if (theBuffer == null) {
			theBuffer = con.processor.getBufferPool().allocate();
			con.readBuffer = theBuffer;
		}
		
		int got = channel.read(theBuffer);
		con.onReadData(got);
	}

}
