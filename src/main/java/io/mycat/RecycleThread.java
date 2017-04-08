package io.mycat;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.buffer.ByteBufferPage;
import io.mycat.buffer.DirectByteBufferPool;
import sun.nio.ch.DirectBuffer;

/**
 * 回收内存池中内存
 * @author huangyiming
 *
 */
public class RecycleThread extends Thread {
	static AtomicInteger count = new AtomicInteger(0);
	@SuppressWarnings("unused")
	private String name;
	public RecycleThread(String name){
		super(name);
		this.name = name;
	}
	protected static final Logger LOGGER = LoggerFactory
			.getLogger(RecycleThread.class);
	@Override
	public void run() {
		int chunkSize = MycatServer.getInstance().getConfig().getSystem().getBufferPoolChunkSize();
	    ConcurrentHashMap<Long,Long> memoryUsage = MycatServer.getInstance().getDirectByteBufferPool().getNetDirectMemoryUsage();
		ByteBufferPage[] allPages = MycatServer.getInstance().getDirectByteBufferPool().getAllPages();
		ByteBuffer theBuf = null;
		 for(;;){
			
			 try {
				while((theBuf = DirectByteBufferPool.noBlockingQueue.take()) !=null){
					// System.out.println("处理第"+count.incrementAndGet()+" 个,相差:"+(DirectByteBufferPool.apply.get()-count.get())+"个");
					 LOGGER.debug("process the "+count.incrementAndGet()+" number,have:"+(DirectByteBufferPool.apply.get()-count.get()));

				    	if(!(theBuf instanceof DirectBuffer)){
				    		theBuf.clear();
				    		return;
				    	}

				    	final long size = theBuf.capacity();

				        boolean recycled = false;
				        DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
				        int chunkCount = theBuf.capacity() / chunkSize;
				        DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
				        int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / chunkSize);
				        for (int i = 0; i < allPages.length; i++) {
				            if ((recycled = allPages[i].recycleBuffer((ByteBuffer) parentBuf, startChunk, chunkCount) == true)) {
				                break;
				            }
				        }
				        final long threadId = Thread.currentThread().getId();

				        if (memoryUsage.containsKey(threadId)){
				            memoryUsage.put(threadId,memoryUsage.get(threadId)-size);
				        }
				        if (recycled == false) {
				            LOGGER.warn("warning ,not recycled buffer " + theBuf);
				        }
				 }
			} catch (InterruptedException e) {
				LOGGER.error("InterruptedException when take from noBlockingQueue",e);
			}
		 }
	}

}
