package io.mycat.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import io.mycat.util.TimeUtil;

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
					disableWrite();
				}

			} else {

				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
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
			try {
				while (buffer.hasRemaining()) {
					written = channel.write(buffer);// java.io.IOException:
									// Connection reset by peer
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
				con.writeBuffer = buffer;
				con.writeAttempts++;
				return false;
			} else {
				con.recycle(buffer);
			}
		}
		return true;
	}

	private void disableWrite() {
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() & OP_NOT_WRITE);
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("can't disable write " + e + " con "
					+ con);
		}

	}

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

	@Override
	public void asynRead() throws IOException {
		ByteBuffer theBuffer = con.readBuffer;
		if (theBuffer == null) {

			theBuffer = con.processor.getBufferPool().allocate(con.processor.getBufferPool().getChunkSize());

			con.readBuffer = theBuffer;
		}

		int got = channel.read(theBuffer);

		con.onReadData(got);
	}

}
