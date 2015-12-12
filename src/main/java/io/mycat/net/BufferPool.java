package io.mycat.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wuzh
 */
public final class BufferPool {
	// this value not changed ,isLocalCacheThread use it
	public static final String LOCAL_BUF_THREAD_PREX = "$_";
	private final ThreadLocalBufferPool localBufferPool;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(BufferPool.class);
	private final int chunkSize;
	private final int conReadBuferChunk;
	private final ConcurrentLinkedQueue<ByteBuffer> items = new ConcurrentLinkedQueue<ByteBuffer>();
	/**
	 * 只用于Connection读取Socket事件，每个Connection一个ByteBuffer（Direct），
	 * 此ByteBufer通常应该能容纳2-N个 应用消息的报文长度，
	 * 对于超出的报文长度，则由BufferPool单独份分配临时的堆内ByteBuffer
	 */
	private final ConcurrentLinkedQueue<ByteBuffer> conReadBuferQueue = new ConcurrentLinkedQueue<ByteBuffer>();
	private long sharedOptsCount;
	private int newCreated;
	private final long threadLocalCount;
	private final long capactiy;

	public BufferPool(long bufferSize, int chunkSize, int conReadBuferChunk,
			int threadLocalPercent) {
		this.chunkSize = chunkSize;
		this.conReadBuferChunk = conReadBuferChunk;
		long size = bufferSize / chunkSize;
		size = (bufferSize % chunkSize == 0) ? size : size + 1;
		this.capactiy = size;
		threadLocalCount = threadLocalPercent * capactiy / 100;
		for (int i = 0; i < capactiy; i++) {
			items.offer(createDirectBuffer(chunkSize));
		}
		localBufferPool = new ThreadLocalBufferPool(threadLocalCount);
	}

	private static final boolean isLocalCacheThread() {
		final String thname = Thread.currentThread().getName();
		return (thname.length() < LOCAL_BUF_THREAD_PREX.length()) ? false
				: (thname.charAt(0) == '$' && thname.charAt(1) == '_');

	}

	public int getConReadBuferChunk() {
		return conReadBuferChunk;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public long getSharedOptsCount() {
		return sharedOptsCount;
	}

	public long size() {
		return this.items.size();
	}

	public long capacity() {
		return capactiy + newCreated;
	}

	public ByteBuffer allocateConReadBuffer() {
		ByteBuffer result = conReadBuferQueue.poll();
		if (result != null) {
			return result;
		} else {
			return createDirectBuffer(conReadBuferChunk);
		}

	}

	public BufferArray allocateArray() {
		return new BufferArray(this);
	}

	public ByteBuffer allocate() {
		ByteBuffer node = null;
		if (isLocalCacheThread()) {
			// allocate from threadlocal
			node = localBufferPool.get().poll();
			if (node != null) {
				return node;
			}
		}
		node = items.poll();
		if (node == null) {
			newCreated++;
			node = this.createDirectBuffer(chunkSize);
		}
		return node;
	}

	private boolean checkValidBuffer(ByteBuffer buffer) {
		// 拒绝回收null和容量大于chunkSize的缓存
		if (buffer == null || !buffer.isDirect()) {
			return false;
		} else if (buffer.capacity() != chunkSize) {
			LOGGER.warn("cant' recycle  a buffer not equals my pool chunksize "
					+ chunkSize + "  he is " + buffer.capacity());
			throw new RuntimeException("bad size");

			// return false;
		}
		buffer.clear();
		return true;
	}

	public void recycleConReadBuffer(ByteBuffer buffer) {
		if (buffer == null || !buffer.isDirect()) {
			return;
		} else if (buffer.capacity() != conReadBuferChunk) {
			LOGGER.warn("cant' recycle  a buffer not equals my pool con read chunksize "
					+ buffer.capacity());
		} else {
			buffer.clear();
			this.conReadBuferQueue.add(buffer);
		}
	}

	public void recycle(ByteBuffer buffer) {
		if (!checkValidBuffer(buffer)) {
			return;
		}
		if (isLocalCacheThread()) {
			BufferQueue localQueue = localBufferPool.get();
			if (localQueue.snapshotSize() < threadLocalCount) {
				localQueue.put(buffer);
			} else {
				// recyle 3/4 thread local buffer
				items.addAll(localQueue.removeItems(threadLocalCount * 3 / 4));
				items.offer(buffer);
				sharedOptsCount++;
			}
		} else {
			sharedOptsCount++;
			items.offer(buffer);
		}

	}

	public boolean testIfDuplicate(ByteBuffer buffer) {
		for (ByteBuffer exists : items) {
			if (exists == buffer) {
				return true;
			}
		}
		return false;

	}

	private ByteBuffer createDirectBuffer(int size) {
		// for performance
		return ByteBuffer.allocateDirect(size);
	}

	public ByteBuffer allocate(int size) {
		if (size <= this.chunkSize) {
			return allocate();
		} else {
			LOGGER.warn("allocate buffer size large than default chunksize:"
					+ this.chunkSize + " he want " + size);
			throw new RuntimeException("execuddd");
			// return createTempBuffer(size);
		}
	}

	public static void main(String[] args) {
		BufferPool pool = new BufferPool(1024 * 5, 1024, 1024 * 3, 2);
		long i = pool.capacity();
		ArrayList<ByteBuffer> all = new ArrayList<ByteBuffer>();
		for (int j = 0; j <= i; j++) {
			all.add(pool.allocate());
		}
		for (ByteBuffer buf : all) {
			pool.recycle(buf);
		}
		System.out.println(pool.size());
	}
}
