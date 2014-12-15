/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.opencloudb.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.NetworkChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.util.TimeUtil;

/**
 * @author mycat
 */
public abstract class AbstractConnection implements NIOConnection {
	protected String host;
	protected int localPort;
	protected int port;
	protected long id;
	protected volatile String charset;
	protected static final Logger LOGGER = Logger
			.getLogger(AbstractConnection.class);
	protected final NetworkChannel channel;
	protected NIOProcessor processor;
	protected NIOHandler handler;

	protected int packetHeaderSize;
	protected int maxPacketSize;
	protected volatile ByteBuffer readBuffer;
	protected volatile ByteBuffer writeBuffer;
	// private volatile boolean writing = false;
	protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();

	protected final AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long netInBytes;
	protected long netOutBytes;
	protected int writeAttempts;

	private long idleTimeout;

	private final SocketWR socketWR;

	public AbstractConnection(NetworkChannel channel) {
		this.channel = channel;
		boolean isAIO = (channel instanceof AsynchronousChannel);
		if (isAIO) {
			socketWR = new AIOSocketWR(this);
		} else {
			socketWR = new NIOSocketWR(this);
		}
		this.isClosed = new AtomicBoolean(false);
		this.startupTime = TimeUtil.currentTimeMillis();
		this.lastReadTime = startupTime;
		this.lastWriteTime = startupTime;
	}

	public String getCharset() {
		return charset;
	}

	public boolean setCharset(String charset) {
		this.charset = charset;
		return true;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public SocketWR getSocketWR() {
		return socketWR;
	}

	public void setIdleTimeout(long idleTimeout) {
		this.idleTimeout = idleTimeout;
	}

	public int getLocalPort() {
		return localPort;
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

	public void setLocalPort(int localPort) {
		this.localPort = localPort;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime,
				lastReadTime) + idleTimeout;

	}

	public NetworkChannel getChannel() {
		return channel;
	}

	public int getPacketHeaderSize() {
		return packetHeaderSize;
	}

	public void setPacketHeaderSize(int packetHeaderSize) {
		this.packetHeaderSize = packetHeaderSize;
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public void setMaxPacketSize(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	public long getStartupTime() {
		return startupTime;
	}

	public long getLastReadTime() {
		return lastReadTime;
	}

	public void setProcessor(NIOProcessor processor) {
		this.processor = processor;
		this.readBuffer = processor.getBufferPool().allocate();
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

	public int getWriteAttempts() {
		return writeAttempts;
	}

	public NIOProcessor getProcessor() {
		return processor;
	}

	public ByteBuffer getReadBuffer() {
		return readBuffer;
	}

	public ByteBuffer allocate() {
		ByteBuffer buffer = this.processor.getBufferPool().allocate();
		return buffer;
	}

	public final void recycle(ByteBuffer buffer) {
		this.processor.getBufferPool().recycle(buffer);
	}

	public void setHandler(NIOHandler handler) {
		this.handler = handler;
	}

	@Override
	public void handle(byte[] data) {
		handler.handle(data);
	}

	@Override
	public void register() throws IOException {

	}

	public void asynRead() throws IOException {

		this.socketWR.asynRead();
	}

	public void doNextWriteCheck() throws IOException {
		this.socketWR.doNextWriteCheck();
	}

	public void onReadData(int got) throws IOException {
		if (isClosed.get()) {
			return;
		}
		ByteBuffer buffer = this.readBuffer;
		lastReadTime = TimeUtil.currentTimeMillis();
		if (got <= 0) {

			if (!this.isClosed()) {
				this.close("socket closed");
				return;
			}
		}
		netInBytes += got;
		processor.addNetInBytes(got);
		// for read byte
		int length = 0;
		int maxReadableLen = 0;

		buffer.flip();
		for (;;) {
			maxReadableLen = buffer.remaining();
			// if(maxReadableLen>100)
			// {
			// System.out.println("readable "+maxReadableLen+
			// " buf "+buffer+" thread "+Thread.currentThread().getId());
			// }
			if (maxReadableLen < this.packetHeaderSize) {
				break;
			}
			length = getPacketLength(buffer, buffer.position());
			if (length > maxPacketSize) {
				throw new IllegalArgumentException(
						"Packet size over the limit " + maxPacketSize);
			} else if (length <= maxReadableLen) {
				byte[] data = new byte[length];
				buffer.get(data);
				// buffer.position(buffer.position()+length);
				handle(data);
			} else {
				// lenth maybe exceed my capacity ,create new
				// next read event
				// System.out.println(" buffer info " +
				// buffer+" thread "+Thread.currentThread().getId());
				buffer = buffer.compact();
				// System.out.println(" buffer info after compact " +
				// buffer+" thread "+Thread.currentThread().getId());
				int curReaded = buffer.position();
				int writableLen = buffer.capacity() - curReaded;
				if (writableLen < length) {
					int chunkSize = processor.getBufferPool().getChunkSize();
					int size = chunkSize + curReaded;
					if (!this.isClosed.get()) {
						// try allocat a new standard buffer size
						size = curReaded
								+ ((chunkSize >= length) ? chunkSize : length);

					}
					ByteBuffer newBuffer = processor.getBufferPool().allocate(
							size);
					buffer.flip();
					newBuffer.put(buffer);
					recycle(buffer);
					readBuffer = newBuffer;
					// System.out.println(" writeable " + writableLen
					// + " readed " + curReaded + " length:" + length
					// + " new Size:" + size+
					// " max readable len "+maxReadableLen+
					// " buf "+buffer+" thread "+Thread.currentThread().getId());

				}
				return;
			}
		}
		// compcat this buffer for next read
		this.readBuffer = buffer.compact();
	}

	public void write(byte[] data) {
		ByteBuffer buffer = allocate();
		buffer = writeToBuffer(data, buffer);
		write(buffer);

	}

	private final void writeNotSend(ByteBuffer buffer) {
		writeQueue.offer(buffer);
	}

	@Override
	public final void write(ByteBuffer buffer) {
		writeQueue.offer(buffer);
		// if ansyn write finishe event got lock before me ,then writing
		// flag is set false but not start a write request
		// so we check again
		try {
			this.socketWR.doNextWriteCheck();
		} catch (Exception e) {
			LOGGER.warn("write err:", e);
			this.close("write err:" + e);

		}

	}

	public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity,
			boolean writeSocketIfFull) {
		if (capacity > buffer.remaining()) {
			if (writeSocketIfFull) {
				writeNotSend(buffer);
				return allocate();
			} else {// Relocate a larger buffer
				buffer.flip();
				ByteBuffer newBuf = ByteBuffer.allocate(capacity);
				newBuf.put(buffer);
				this.recycle(buffer);
				return newBuf;
			}
		} else {
			return buffer;
		}
	}

	public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
		int offset = 0;
		int length = src.length;
		int remaining = buffer.remaining();
		while (length > 0) {
			if (remaining >= length) {
				buffer.put(src, offset, length);
				break;
			} else {
				buffer.put(src, offset, remaining);
				writeNotSend(buffer);
				buffer = allocate();
				offset += remaining;
				length -= remaining;
				remaining = buffer.remaining();
				continue;
			}
		}
		return buffer;
	}

	@Override
	public void close(String reason) {
		if (!isClosed.get()) {
			closeSocket();
			isClosed.set(true);
			this.cleanup();
			LOGGER.info("close connection,reason:" + reason + " ," + this);
		}
	}

	public boolean isClosed() {
		return isClosed.get();
	}

	public void idleCheck() {
		if (isIdleTimeout()) {
			LOGGER.info(toString() + " idle timeout");
			close(" idle ");
		}
	}

	/**
	 * 娓呯悊閬楃暀璧勬簮
	 */
	protected void cleanup() {

		// 鍥炴敹鎺ユ敹缂撳瓨
		if (readBuffer != null) {
			recycle(readBuffer);
			this.readBuffer = null;
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

	protected final int getPacketLength(ByteBuffer buffer, int offset) {

		int length = buffer.get(offset) & 0xff;
		length |= (buffer.get(++offset) & 0xff) << 8;
		length |= (buffer.get(++offset) & 0xff) << 16;
		return length + packetHeaderSize;

	}

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
		return writeQueue;
	}

	private void closeSocket() {

		if (channel != null) {
			boolean isSocketClosed = true;
			try {
				channel.close();
				this.socketWR.close();
			} catch (Throwable e) {
			}
			boolean closed = isSocketClosed && (!channel.isOpen());
			if (closed == false) {
				LOGGER.warn("close socket of connnection failed " + this);
			}

		}
	}

}
