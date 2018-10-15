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
package io.mycat.net;

import com.google.common.base.Strings;
import io.mycat.backend.mysql.CharsetUtil;
import io.mycat.util.CompressUtil;
import io.mycat.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NIO抽象连接
 *
 * @author mycat
 */
public abstract class AbstractConnection implements NIOConnection {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnection.class);
	
	protected String host;
	protected int localPort;
	protected int port;
	protected long id;
	protected volatile String charset;
	protected volatile int charsetIndex;

	protected final NetworkChannel channel;
	protected NIOProcessor processor;
	protected NIOHandler handler;

	// 包头大小
	protected int packetHeaderSize;
	// 最大包大小
	protected int maxPacketSize;
	// 读缓存
	protected volatile ByteBuffer readBuffer;
	// 写缓存
	protected volatile ByteBuffer writeBuffer;
	
	protected final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	// 读缓存偏移量
	protected volatile int readBufferOffset;
	// 最后信息时间
	protected long lastLargeMessageTime;
	protected final AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	// 启动时间
	protected long startupTime;
	// 最后读时间
	protected long lastReadTime;
	// 最后写时间
	protected long lastWriteTime;
	// 网络流入字节大小
	protected long netInBytes;
	// 网络流出字节大小
	protected long netOutBytes;
	// 写尝试次数，
	protected int writeAttempts;
	// 支持压缩标识
	protected volatile boolean isSupportCompress = false;
	// 解压缩未完成的数据队列
    protected final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<byte[]>();
    // 压缩未完成的数据队列
    protected final ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue = new ConcurrentLinkedQueue<byte[]>();
    // 空闲超时时间
	private long idleTimeout;

	private final SocketWR socketWR;

	public AbstractConnection(NetworkChannel channel) {
		this.channel = channel;
		boolean isAIO = (channel instanceof AsynchronousChannel);
		if (isAIO) { // AIO
			socketWR = new AIOSocketWR(this);
		} else { // NIO
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
		// 修复PHP字符集设置错误, 如： set names 'utf8'
		if (charset != null) {
			charset = charset.replaceAll("'", "");
		}

		int ci = CharsetUtil.getIndex(charset);
		if (ci > 0) {
			this.charset = charset.equalsIgnoreCase("utf8mb4") ? "utf8" : charset;
			this.charsetIndex = ci;
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 是否支持压缩
	 * @return
	 */
	public boolean isSupportCompress() {
		return isSupportCompress;
	}

	public void setSupportCompress(boolean isSupportCompress) {
		this.isSupportCompress = isSupportCompress;
	}

	public int getCharsetIndex() {
		return charsetIndex;
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

	/**
	 * 是否空闲超时
	 * @return
	 */
	public boolean isIdleTimeout() {
		return TimeUtil.currentTimeMillis() > Math.max(lastWriteTime, lastReadTime) + idleTimeout;
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
		int size = processor.getBufferPool().getChunkSize();
		this.readBuffer = processor.getBufferPool().allocate(size);
	}

	public long getLastWriteTime() {
		return lastWriteTime;
	}
	
	public void setLastWriteTime(long lasttime){
		this.lastWriteTime = lasttime;
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

	/**
	 * 创建缓存
	 * @return
	 */
	public ByteBuffer allocate() {
		int size = this.processor.getBufferPool().getChunkSize();
		ByteBuffer buffer = this.processor.getBufferPool().allocate(size);
		return buffer;
	}

	/**
	 * 回收资源
	 * @param buffer
	 */
	public final void recycle(ByteBuffer buffer) {
		this.processor.getBufferPool().recycle(buffer);
	}

	public void setHandler(NIOHandler handler) {
		this.handler = handler;
	}

	@Override
	public void handle(byte[] data) {
		if (isSupportCompress()) {
			// 支持压缩
			// 解压数据
			List<byte[]> packs = CompressUtil.decompressMysqlPacket(data, decompressUnfinishedDataQueue);
			for (byte[] pack : packs) {
				if (pack.length != 0) {
					handler.handle(pack);
				}
			}
		} else {
			handler.handle(data);
		}
	}
	
	@Override
	public void register() throws IOException {

	}

	/**
	 * 异步读
	 * @throws IOException
	 */
	public void asynRead() throws IOException {
		this.socketWR.asynRead();
	}

	/**
	 * 执行下一个写检查
	 * @throws IOException
	 */
	public void doNextWriteCheck() throws IOException {
		this.socketWR.doNextWriteCheck();
	}

	/**
	 * 读取可能的Socket字节流
	 * @param got 读取到的数据长度， - 1表示已经读到了流的末尾（连接关闭了）
	 * @throws IOException
	 */
	public void onReadData(int got) throws IOException {
		if (isClosed.get()) {
			// 连接已经关闭
			return;
		}
		
		lastReadTime = TimeUtil.currentTimeMillis();
		if (got < 0) {
			this.close("stream closed");
            return;
		} else if (got == 0
				&& !this.channel.isOpen()) {
				this.close("socket closed");
				return;
		}
		netInBytes += got;
		processor.addNetInBytes(got);

		// 循环处理字节信息
		int offset = readBufferOffset, length = 0, position = readBuffer.position();
		for (;;) {
			length = getPacketLength(readBuffer, offset);			
			if (length == -1) {
				if (offset != 0) {
					this.readBuffer = compactReadBuffer(readBuffer, offset);
				} else if (readBuffer != null && !readBuffer.hasRemaining()) {
					// 读取缓冲区容量无效，缓冲区大小太小
					throw new RuntimeException( "invalid readbuffer capacity ,too little buffer size " 
							+ readBuffer.capacity());
				}
				break;
			}

			if (position >= offset + length && readBuffer != null) {
				// 处理这个包
				readBuffer.position(offset);				
				byte[] data = new byte[length];
				readBuffer.get(data, 0, length);
				handle(data);
				
				if(isClosed()) {
					// 连接已经关闭
					return ;
				}

				// 偏移到下一个位置
				offset += length;
				
				// 处理完
				if (position == offset) {
					// 如果当前缓冲区是临时的，则没有直接字节缓冲区，并且在最近30秒内没有收到大的消息，更改为直接缓冲区以获得性能
					if (readBuffer != null && !readBuffer.isDirect()
							&& lastLargeMessageTime < lastReadTime - 30 * 1000L) {  // used temp heap
						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("change to direct con read buffer ,cur temp buf size :" + readBuffer.capacity());
						}
						recycle(readBuffer);
						readBuffer = processor.getBufferPool().allocate(processor.getBufferPool().getConReadBuferChunk());
					} else {
						if (readBuffer != null) {
							readBuffer.clear();
						}
					}
					// 没有更多的数据
					readBufferOffset = 0;
					break;
				} else {
					// 尝试解析下一个包
					readBufferOffset = offset;
					if(readBuffer != null) {
						readBuffer.position(position);
					}
					continue;
				}
			} else {
				// 没有读取整个消息包，所以检查是否有足够的缓冲区和压缩的readbuffer
				if (!readBuffer.hasRemaining()) {
					readBuffer = ensureFreeSpaceOfReadBuffer(readBuffer, offset, length);
				}
				break;
			}
		}
	}
	
	private boolean isConReadBuffer(ByteBuffer buffer) {
		return buffer.capacity() == processor.getBufferPool().getConReadBuferChunk() && buffer.isDirect();
	}
	
	private ByteBuffer ensureFreeSpaceOfReadBuffer(ByteBuffer buffer,
			int offset, final int pkgLength) {
		// 需要一个大的缓冲区来保存包
		if (pkgLength > maxPacketSize) {
			//包大小超过限制
			throw new IllegalArgumentException("Packet size over the limit.");
		} else if (buffer.capacity() < pkgLength) {
			// 包大小比缓冲区的容量大，创建新的缓冲区
			ByteBuffer newBuffer = processor.getBufferPool().allocate(pkgLength);
			lastLargeMessageTime = TimeUtil.currentTimeMillis();
			buffer.position(offset);
			newBuffer.put(buffer);
			readBuffer = newBuffer;

			recycle(buffer);
			readBufferOffset = 0;
			return newBuffer;

		} else {
			if (offset != 0) {
				// 压缩缓冲区
				return compactReadBuffer(buffer, offset);
			} else {
				throw new RuntimeException(" not enough space");
			}
		}
	}

	/**
	 * 压缩这个缓冲区
	 * @param buffer
	 * @param offset
	 * @return
	 */
	private ByteBuffer compactReadBuffer(ByteBuffer buffer, int offset) {
		if(buffer == null) {
			return null;
		}
		buffer.limit(buffer.position());
		buffer.position(offset);
		buffer = buffer.compact();
		readBufferOffset = 0;
		return buffer;
	}

	public void write(byte[] data) {
		ByteBuffer buffer = allocate();
		buffer = writeToBuffer(data, buffer);
		write(buffer);
	}

	private final void writeNotSend(ByteBuffer buffer) {
		if (isSupportCompress()) {
			ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
			writeQueue.offer(newBuffer);
		} else {
			writeQueue.offer(buffer);
		}
		
		if(isClosed()) {
			LOGGER.warn("write err:{}", this);
			this.close("found this connection has close , try to reClean the connection");
			throw new RuntimeException("writeNotSend but found connnection close err:" + this);
		}
	}

    @Override
	public final void write(ByteBuffer buffer) {
		if (isSupportCompress()) {
			ByteBuffer newBuffer = CompressUtil.compressMysqlPacket(buffer, this, compressUnfinishedDataQueue);
			writeQueue.offer(newBuffer);
		} else {
			writeQueue.offer(buffer);
		}

		// 如果异步写完成事件在我之前被锁定，那么写入标志设置为false，但不启动写请求，所以我们再次检查
		try {
			this.socketWR.doNextWriteCheck();
		} catch (Exception e) {
			LOGGER.warn("write err:", e);
			this.close("write err:" + e);
		}
	}

	public ByteBuffer checkWriteBuffer(ByteBuffer buffer, int capacity, boolean writeSocketIfFull) {
		if (capacity > buffer.remaining()) {
			if (writeSocketIfFull) {
				writeNotSend(buffer);
				return processor.getBufferPool().allocate(capacity);
			} else {
				// 安置一个更大的缓冲
				buffer.flip();
				ByteBuffer newBuf = processor.getBufferPool().allocate(capacity + buffer.limit() + 1);
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
			if (processor != null) {
				processor.removeConnection(this);
			}
			this.cleanup();
			isSupportCompress = false;

			// 忽略空信息
			if (Strings.isNullOrEmpty(reason)) {
				return;
			}
			LOGGER.info("close connection,reason:" + reason + " ," + this);
			if (reason.contains("connection,reason:java.net.ConnectException")) {
				throw new RuntimeException(" errr");
			}
		} else {
		    // 再次确保清理
		    // Fix issue#1616
		    this.cleanup();
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
	 * 清理资源
	 */
	protected void cleanup() {
		// 清理资源占用
		if (readBuffer != null) {
			this.recycle(readBuffer);
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		
		if (writeBuffer != null) {
			recycle(writeBuffer);
			this.writeBuffer = null;
		}
		
		if (!decompressUnfinishedDataQueue.isEmpty()) {
			decompressUnfinishedDataQueue.clear();
		}
		
		if (!compressUnfinishedDataQueue.isEmpty()) {
			compressUnfinishedDataQueue.clear();
		}
		
		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}
	
	protected int getPacketLength(ByteBuffer buffer, int offset) {
		int headerSize = getPacketHeaderSize();
		if ( isSupportCompress() ) {
			headerSize = 7;
		}
		
		if (buffer.position() < offset + headerSize) {
			return -1;
		} else {
			int length = buffer.get(offset) & 0xff;
			length |= (buffer.get(++offset) & 0xff) << 8;
			length |= (buffer.get(++offset) & 0xff) << 16;
			return length + headerSize;
		}
	}

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
		return writeQueue;
	}

	private void closeSocket() {
		if (channel != null) {
			if (channel instanceof SocketChannel) {
				Socket socket = ((SocketChannel) channel).socket();
				if (socket != null) {
					try {
						socket.close();
					} catch (IOException e) {
				       LOGGER.error("closeChannelError", e);
					}
				}
			}
				
			boolean isSocketClosed = true;
			try {
				channel.close();
			} catch (Exception e) {
				LOGGER.error("AbstractConnectionCloseError", e);
			}
			
			boolean closed = isSocketClosed && (!channel.isOpen());
			if (closed == false) {
				LOGGER.warn("close socket of connnection failed " + this);
			}
		}
	}
	public void onConnectfinish() {
		LOGGER.debug("连接后台真正完成");
	}	
}
