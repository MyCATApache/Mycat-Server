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
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Strings;
import org.apache.log4j.Logger;
import org.opencloudb.mysql.CharsetUtil;
import org.opencloudb.util.CompressUtil;
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
	protected volatile int charsetIndex;
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
	protected volatile int readBufferOffset;
	protected final AtomicBoolean isClosed;
	protected boolean isSocketClosed;
	protected long startupTime;
	protected long lastReadTime;
	protected long lastWriteTime;
	protected long netInBytes;
	protected long netOutBytes;
	protected int writeAttempts;
	protected volatile boolean isSupportCompress=false;
    protected final ConcurrentLinkedQueue<byte[]> decompressUnfinishedDataQueue = new ConcurrentLinkedQueue<byte[]>();
    protected final ConcurrentLinkedQueue<byte[]> compressUnfinishedDataQueue = new ConcurrentLinkedQueue<byte[]>();

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
		
		//修复PHP字符集设置错误, 如： set names 'utf8'
		if ( charset != null ) {			
			charset = charset.replace("'", "");
		}
		
		int ci = CharsetUtil.getIndex(charset);
		if (ci > 0) {
			this.charset = charset.equalsIgnoreCase("utf8mb4")?"utf8":charset;
			this.charsetIndex = ci;
			return true;
		} else {
			return false;
		}
	}




    public boolean isSupportCompress()
	{
		return isSupportCompress;
	}

	public void setSupportCompress(boolean isSupportCompress)
	{
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
        if(isSupportCompress())
        {
            List<byte[]> packs= CompressUtil.decompressMysqlPacket(data,decompressUnfinishedDataQueue);

            for (byte[] pack : packs)
            {
				if(pack.length != 0)
                handler.handle(pack);
            }
        }   else
        {
            handler.handle(data);
        }
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
		processor.addNetInBytes(got);

		// 循环处理字节信息
		int offset = readBufferOffset, length = 0, position = buffer.position();
		for (;;) {
			length = getPacketLength(buffer, offset);
			if (length == -1) {
				if (!buffer.hasRemaining()) {
					buffer = checkReadBuffer(buffer, offset, position);
				}
				break;
			}
			if (position >= offset + length) {
				buffer.position(offset);
				byte[] data = new byte[length];
				buffer.get(data, 0, length);
				handle(data);

				offset += length;
				if (position == offset) {
					if (readBufferOffset != 0) {
						readBufferOffset = 0;
					}
					buffer.clear();
					break;
				} else {
					readBufferOffset = offset;
					buffer.position(position);
					continue;
				}
			} else {
				if (!buffer.hasRemaining()) {
					buffer = checkReadBuffer(buffer, offset, position);
				}
				break;
			}
		}
	}

	private ByteBuffer checkReadBuffer(ByteBuffer buffer, int offset,
			int position) {
		if (offset == 0) {
			if (buffer.capacity() >= maxPacketSize) {
				throw new IllegalArgumentException(
						"Packet size over the limit.");
			}
			int size = buffer.capacity() << 1;
			size = (size > maxPacketSize) ? maxPacketSize : size;
			ByteBuffer newBuffer = processor.getBufferPool().allocate(size);
			buffer.position(offset);
			newBuffer.put(buffer);
			readBuffer = newBuffer;
			recycle(buffer);
			return newBuffer;
		} else {
			buffer.position(offset);
			buffer.compact();
			readBufferOffset = 0;
			return buffer;
		}
	}

	public void write(byte[] data) {
		ByteBuffer buffer = allocate();
		buffer = writeToBuffer(data, buffer);
		write(buffer);

	}

	private final void writeNotSend(ByteBuffer buffer) {
        if(isSupportCompress())
        {
            ByteBuffer     newBuffer= CompressUtil.compressMysqlPacket(buffer,this,compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);
        }   else
        {
            writeQueue.offer(buffer);
        }
	}


    @Override
	public final void write(ByteBuffer buffer) {
        if(isSupportCompress())
        {
            ByteBuffer     newBuffer= CompressUtil.compressMysqlPacket(buffer,this,compressUnfinishedDataQueue);
            writeQueue.offer(newBuffer);

        }   else
        {
            writeQueue.offer(buffer);
        }

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
				return processor.getBufferPool().allocate(capacity);
			} else {// Relocate a larger buffer
				buffer.flip();
				ByteBuffer newBuf = processor.getBufferPool().allocate(
						capacity + buffer.limit() + 1);
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
			isSupportCompress=false;

			//ignore null information
			if (Strings.isNullOrEmpty(reason)) {
				return;
			}
			LOGGER.info("close connection,reason:" + reason + " ," + this);
			if (reason.contains("connection,reason:java.net.ConnectException")) {
				throw new RuntimeException(" errr");
			}
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

		if (readBuffer != null) {
			recycle(readBuffer);
			this.readBuffer = null;
			this.readBufferOffset = 0;
		}
		if (writeBuffer != null) {
			recycle(writeBuffer);
			this.writeBuffer = null;
		}
        if(!decompressUnfinishedDataQueue.isEmpty())
        {
            decompressUnfinishedDataQueue.clear();
        }
        if(!compressUnfinishedDataQueue.isEmpty())
        {
            compressUnfinishedDataQueue.clear();
        }
		ByteBuffer buffer = null;
		while ((buffer = writeQueue.poll()) != null) {
			recycle(buffer);
		}
	}

	protected final int getPacketLength(ByteBuffer buffer, int offset) {
     int   headerSize  =getPacketHeaderSize();
        if(isSupportCompress())
        {
           headerSize=7;
        }


		if (buffer.position() < offset + headerSize) {
			return -1;
		} else {

			int length = buffer.get(offset) & 0xff;
			length |= (buffer.get(++offset) & 0xff) << 8;
			length |= (buffer.get(++offset) & 0xff) << 16;
//           if( buffer.position()==length+4 )
//           {
//                return   length+4;
//           }
			return length + headerSize;
		}
	}

	public ConcurrentLinkedQueue<ByteBuffer> getWriteQueue() {
		return writeQueue;
	}

	private void closeSocket() {

		if (channel != null) {
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

}
