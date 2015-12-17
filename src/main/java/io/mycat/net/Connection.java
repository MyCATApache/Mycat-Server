package io.mycat.net;

import io.mycat.util.TimeUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wuzh
 */
public abstract class Connection implements ClosableConnection{
	public static Logger LOGGER = LoggerFactory.getLogger(Connection.class);
	protected String host;
	protected int port;
	protected int localPort;
	protected long id;

	public enum State {
		connecting, connected, closing, closed, failed
	}

	private State state = State.connecting;

	// 连接的方向，in表示是客户端连接过来的，out表示自己作为客户端去连接对端Sever
	public enum Direction {
		in, out
	}

	private Direction direction = Direction.in;

	protected final SocketChannel channel;

	private SelectionKey processKey;
	private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
	private ByteBuffer readBuffer;
	private ByteBuffer writeBuffer;
	private final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	private final ReentrantLock writeQueueLock = new ReentrantLock();
	private int readBufferOffset;
	private long lastLargeMessageTime;
	protected boolean isClosed;
	protected boolean isSocketClosed;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected int netInBytes;
	protected int netOutBytes;
	protected int pkgTotalSize;
	protected int pkgTotalCount;
	private long idleTimeout;
	private long lastPerfCollectTime;
	@SuppressWarnings("rawtypes")
	protected NIOHandler handler;
	private int maxPacketSize;
	private int packetHeaderSize;

	public Connection(SocketChannel channel) {
		this.channel = channel;
		this.isClosed = false;
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
		this.lastPerfCollectTime = startupTime;
	}

	public void resetPerfCollectTime() {
		netInBytes = 0;
		netOutBytes = 0;
		pkgTotalCount = 0;
		pkgTotalSize = 0;
		lastPerfCollectTime = TimeUtil.currentTimeMillis();
	}

	public long getLastPerfCollectTime() {
		return lastPerfCollectTime;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public long getId() {
		return id;
	}

	public int getLocalPort() {
		return localPort;
	}

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
				lastReadTime) + idleTimeout;

	}

	public SocketChannel getChannel() {
		return channel;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}

	public long getNetInBytes() {
		return netInBytes;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public ByteBuffer getReadBuffer() {
		return readBuffer;
	}

	private ByteBuffer allocate() {
		ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate();
		return buffer;
	}

	private final void recycle(ByteBuffer buffer) {
		NetSystem.getInstance().getBufferPool().recycle(buffer);
	}

	public void setHandler(NIOHandler<? extends Connection> handler) {
		this.handler = handler;

	}

	@SuppressWarnings("rawtypes")
	public NIOHandler getHandler() {
		return this.handler;
	}

	@SuppressWarnings("unchecked")
	public void handle(final ByteBuffer data, final int start,
			final int readedLength) {
		handler.handle(this, data, start, readedLength);
	}

	/**
	 * 读取可能的Socket字节流
	 * 
	 * @param got
	 * @throws IOException
	 */
	public void onReadData(int got) throws IOException {
		if (isClosed) {
			return;
		}
		lastReadTime = TimeUtil.currentTimeMillis();
		if (got < 0) {
			this.close("stream closed");
			return;
		} else if (got == 0) {
			if (!this.channel.isOpen()) {
				this.close("socket closed");
				return;
			}
		}
		netInBytes += got;
		// System.out.println("readed new  size "+got);
		NetSystem.getInstance().addNetInBytes(got);

		// 循环处理字节信息
		int offset = readBufferOffset, length = 0, position = readBuffer
				.position();
		for (;;) {
			length = getPacketLength(readBuffer, offset, position);
			// LOGGER.info("message lenth "+length+" offset "+offset+" positon "+position+" capactiy "+readBuffer.capacity());
			// System.out.println("message lenth "+length+" offset "+offset+" positon "+position);
			if (length == -1) {
				if (offset != 0) {
					this.readBuffer = compactReadBuffer(readBuffer, offset);
				} else if (!readBuffer.hasRemaining()) {
					throw new RuntimeException(
							"invalid readbuffer capacity ,too little buffer size "
									+ readBuffer.capacity());
				}
				break;
			}
			pkgTotalCount++;
			pkgTotalSize += length;
			// check if a complete message packge received
			if (offset + length <= position) {
				// handle this package
				readBuffer.position(offset);
				handle(readBuffer, offset, length);
				
				// maybe handle stmt_close
				if(isClosed()) {
					return ;
				}
				
				// offset to next position
				offset += length;
				// reached end
				if (position == offset) {
					// if cur buffer is temper none direct byte buffer and not
					// received large message in recent 30 seconds
					// then change to direct buffer for performance
					if (!readBuffer.isDirect()
							&& lastLargeMessageTime < lastReadTime - 30 * 1000L) {// used
																					// temp
																					// heap
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("change to direct con read buffer ,cur temp buf size :"
									+ readBuffer.capacity());
						}
						recycle(readBuffer);
						readBuffer = NetSystem.getInstance().getBufferPool()
								.allocateConReadBuffer();
					} else {
						readBuffer.clear();
					}
					// no more data ,break
					readBufferOffset = 0;
					break;
				} else {
					// try next package parse
					readBufferOffset = offset;
					readBuffer.position(position);
					continue;
				}
			} else {
				// not read whole message package ,so check if buffer enough and
				// compact readbuffer
				if (!readBuffer.hasRemaining()) {
					readBuffer = ensureFreeSpaceOfReadBuffer(readBuffer,
							offset, length);
				}
				break;
			}
		}
	}

	public boolean isConnected() {
		return (this.state == Connection.State.connected);
	}

	private boolean isConReadBuffer(ByteBuffer buffer) {
		return buffer.capacity() == NetSystem.getInstance().getBufferPool()
				.getConReadBuferChunk()
				&& buffer.isDirect();
	}

	private ByteBuffer ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
			int offset, final int pkgLength) {
		// need a large buffer to hold the package
		if (pkgLength > maxPacketSize) {
			throw new IllegalArgumentException("Packet size over the limit.");
		} else if (buffer.capacity() < pkgLength) {
			ByteBuffer newBuffer = NetSystem.getInstance().getBufferPool()
					.allocate(pkgLength);
			lastLargeMessageTime = TimeUtil.currentTimeMillis();
			buffer.position(offset);
			newBuffer.put(buffer);
			readBuffer = newBuffer;
			if (isConReadBuffer(buffer)) {
				NetSystem.getInstance().getBufferPool()
						.recycleConReadBuffer(buffer);
			} else {
				recycle(buffer);
			}
			readBufferOffset = 0;
			return newBuffer;

		} else {
			if (offset != 0) {
				// compact bytebuffer only
				return compactReadBuffer(buffer, offset);
			} else {
				throw new RuntimeException(" not enough space");
			}
		}
	}

	private ByteBuffer compactReadBuffer(ByteBuffer buffer, int offset) {
		buffer.limit(buffer.position());
		buffer.position(offset);
		buffer = buffer.compact();
		readBufferOffset = 0;
		return buffer;
	}

	public void write(byte[] src) {
		try {
			writeQueueLock.lock();
			ByteBuffer buffer = this.allocate();
			int offset = 0;
			int remains = src.length;
			while (remains > 0) {
				int writeable = buffer.remaining();
				if (writeable >= remains) {
					// can write whole srce
					buffer.put(src, offset, remains);
					this.writeQueue.offer(buffer);
					break;
				} else {
					// can write partly
					buffer.put(src, offset, writeable);
					offset += writeable;
					remains -= writeable;
					writeQueue.offer(buffer);
					buffer = allocate();
					continue;
				}

			}
		} finally {
			writeQueueLock.unlock();
		}
		this.enableWrite(true);
	}

	/**
	 * note only use this method when the input buffer is shared
	 * 
	 * @param buffer
	 * @param from
	 * @param lenth
	 */
	public final void write(ByteBuffer buffer, int from, int lenth) {
		try {
			writeQueueLock.lock();
			buffer.position(from);
			int remainByts = lenth;
			while (remainByts > 0) {
				ByteBuffer newBuf = allocate();
				int batchSize = newBuf.capacity();
				for (int i = 0; i < batchSize & remainByts > 0; i++) {
					newBuf.put(buffer.get());
					remainByts--;
				}
				writeQueue.offer(newBuf);
			}
		} finally {
			writeQueueLock.unlock();
		}
		this.enableWrite(true);

	}

	public final void write(ByteBuffer buffer) {
		try {
			writeQueueLock.lock();
			writeQueue.offer(buffer);
		} finally {
			writeQueueLock.unlock();
		}
		this.enableWrite(true);
	}

	@SuppressWarnings("unchecked")
	public void close(String reason) {
		if (!isClosed) {
			closeSocket();
			this.cleanup();
			isClosed = true;
			NetSystem.getInstance().removeConnection(this);
			LOGGER.info("close connection,reason:" + reason + " ," + this);
			if (handler != null) {
				handler.onClosed(this, reason);
			}
		}
	}

	/**
	 * asyn close (executed later in thread)
	 * 
	 * @param reason
	 */
	public void asynClose(final String reason) {
		Runnable runn = new Runnable() {
			public void run() {
				Connection.this.close(reason);
			}
		};
		NetSystem.getInstance().getTimer().schedule(runn, 1, TimeUnit.SECONDS);

	}

	public boolean isClosed() {
		return isClosed;
	}

	public void idleCheck() {
		if (isIdleTimeout()) {
			LOGGER.info(toString() + " idle timeout");
			close(" idle ");
		}
	}

	/**
	 * 清理资源
	 */

	protected void cleanup() {

		// 清理资源占用
		if (readBuffer != null) {
			if (isConReadBuffer(readBuffer)) {
				NetSystem.getInstance().getBufferPool()
						.recycleConReadBuffer(readBuffer);

			} else {
				this.recycle(readBuffer);
			}
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		if (writeBuffer != null) {
			recycle(writeBuffer);
			this.writeBuffer = null;
		}

		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}

	protected final int getPacketLength(ByteBuffer buffer, int offset,
			int position) {
		if (position < offset + packetHeaderSize) {
			return -1;
		} else {
			int length = buffer.get(offset) & 0xff;
			length |= (buffer.get(++offset) & 0xff) << 8;
			length |= (buffer.get(++offset) & 0xff) << 16;
			return length + packetHeaderSize;
		}
	}

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
		return writeQueue;
	}

	@SuppressWarnings("unchecked")
	public void register(Selector selector) throws IOException {
		processKey = channel.register(selector, SelectionKey.OP_READ, this);
		NetSystem.getInstance().addConnection(this);
		readBuffer = NetSystem.getInstance().getBufferPool()
				.allocateConReadBuffer();
		this.handler.onConnected(this);

	}

	public void doWriteQueue() {
		try {
			boolean noMoreData = write0();
			lastWriteTime = TimeUtil.currentTimeMillis();
			if (noMoreData && writeQueue.isEmpty()) {
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
					disableWrite();
				}

			} else {

				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
					enableWrite(false);
				}
			}

		} catch (IOException e) {
			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("caught err:", e);
			}
			close("err:" + e);
		}

	}

	public void write(BufferArray bufferArray) {
		try {
			writeQueueLock.lock();
			List<ByteBuffer> blockes = bufferArray.getWritedBlockLst();
			if (!bufferArray.getWritedBlockLst().isEmpty()) {
				for (ByteBuffer curBuf : blockes) {
					writeQueue.offer(curBuf);
				}
			}
			ByteBuffer curBuf = bufferArray.getCurWritingBlock();
			if (curBuf.position() == 0) {// empty
				this.recycle(curBuf);
			} else {
				writeQueue.offer(curBuf);
			}
		} finally {
			writeQueueLock.unlock();
			bufferArray.clear();
		}
		this.enableWrite(true);

	}

	private boolean write0() throws IOException {

		int written = 0;
		ByteBuffer buffer = writeBuffer;
		if (buffer != null) {
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				if (written > 0) {
					netOutBytes += written;
					NetSystem.getInstance().addNetOutBytes(written);

				} else {
					break;
				}
			}

			if (buffer.hasRemaining()) {
				return false;
			} else {
				writeBuffer = null;
				recycle(buffer);
			}
		}
		while ((buffer = writeQueue.poll()) != null) {
			if (buffer.limit() == 0) {
				recycle(buffer);
				close("quit send");
				return true;
			}
			buffer.flip();
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				if (written > 0) {
					netOutBytes += written;
					NetSystem.getInstance().addNetOutBytes(written);
					lastWriteTime = TimeUtil.currentTimeMillis();
				} else {
					break;
				}
			}
			if (buffer.hasRemaining()) {
				writeBuffer = buffer;
				return false;
			} else {
				recycle(buffer);
			}
		}
		return true;
	}

	private void disableWrite() {
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() & OP_NOT_WRITE);
		} catch (Exception e) {
			LOGGER.warn("can't disable write " + e + " con " + this);
		}

	}

	public void enableWrite(boolean wakeup) {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			needWakeup = true;
		} catch (Exception e) {
			LOGGER.warn("can't enable write " + e);

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
			LOGGER.warn("enable read fail " + e);
		}
		if (needWakeup) {
			processKey.selector().wakeup();
		}
	}

	public void setState(State newState) {
		this.state = newState;
	}

	/**
	 * 异步读取数据,only nio thread call
	 * 
	 * @throws IOException
	 */
	protected void asynRead() throws IOException {
		if (this.isClosed) {
			return;
		}
		int got = channel.read(readBuffer);
		onReadData(got);

	}

	private void closeSocket() {

		if (channel != null) {
			boolean isSocketClosed = true;
			try {
				processKey.cancel();
				channel.close();
			} catch (Throwable e) {
			}
			boolean closed = isSocketClosed && (!channel.isOpen());
			if (closed == false) {
				LOGGER.warn("close socket of connnection failed " + this);
			}

		}
	}

	public State getState() {
		return state;
	}

	public Direction getDirection() {
		return direction;
	}

	public void setDirection(Connection.Direction in) {
		this.direction = in;

	}

	public int getPkgTotalSize() {
		return pkgTotalSize;
	}

	public int getPkgTotalCount() {
		return pkgTotalCount;
	}

	@Override
	public String toString() {
		return "Connection [host=" + host + ",  port=" + port + ", id=" + id
				+ ", state=" + state + ", direction=" + direction
				+ ", startupTime=" + startupTime + ", lastReadTime="
				+ lastReadTime + ", lastWriteTime=" + lastWriteTime + "]";
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;

	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;

	}

}
