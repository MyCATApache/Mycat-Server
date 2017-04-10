package io.mycat.buffer.handler;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.buffer.ByteBufferPage;
import io.mycat.buffer.DirectByteBufferPool;
import sun.nio.ch.DirectBuffer;

/**
 * 异步队列回收堆外内存
 * 
 * @author huangyiming@globalegrow.com
 *
 */
public class RecycleThread implements Runnable {
	// 回收bytebuffer数
	private static AtomicInteger recycleCount = new AtomicInteger(0);

	@SuppressWarnings("unused")
	private String name;

	public RecycleThread(String name) {
		this.name = name;
	}

	protected static final Logger LOGGER = LoggerFactory.getLogger(RecycleThread.class);

	@Override
	public void run() {
		int chunkSize = MycatServer.getInstance().getConfig().getSystem().getBufferPoolChunkSize();
		ConcurrentHashMap<Long, Long> memoryUsage = MycatServer.getInstance().getDirectByteBufferPool()
				.getNetDirectMemoryUsage();
		ByteBufferPage[] allPages = MycatServer.getInstance().getDirectByteBufferPool().getAllPages();
		ByteBuffer theBuf = null;
		for (;;) {

			try {
				// it won't be block until the queue has no bytebuffer in it
				while ((theBuf = DirectByteBufferPool.noBlockingQueue.take()) != null) {
					LOGGER.debug("recycleThread process : " + recycleCount.incrementAndGet() + " count,have : "
							+ (DirectByteBufferPool.apply.get() - recycleCount.get() + " no deal"));

					
					final long size = theBuf.capacity();

					boolean recycled = false;
					DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
					int chunkCount = theBuf.capacity() / chunkSize;
					DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
					int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / chunkSize);
					for (int i = 0; i < allPages.length; i++) {
						if ((recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk,
								chunkCount) == true)) {
							break;
						}
					}
					final long threadId = Thread.currentThread().getId();

					if (memoryUsage.containsKey(threadId)) {
						memoryUsage.put(threadId, memoryUsage.get(threadId) - size);
					}
					if (recycled == false) {
						LOGGER.warn("warning ,not recycled buffer " + theBuf);
					}
				}
			} catch (InterruptedException e) {
				LOGGER.error("InterruptedException when take from noBlockingQueue", e);
			}
		}
	}

}
