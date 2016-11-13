package io.mycat.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.nio.ch.DirectBuffer;

/**
 * DirectByteBuffer池，可以分配任意指定大小的DirectByteBuffer，用完需要归还
 * @author wuzhih
 * @author zagnix
 */
@SuppressWarnings("restriction")
public class DirectByteBufferPool implements BufferPool{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirectByteBufferPool.class);
    public static final String LOCAL_BUF_THREAD_PREX = "$_";
    private ByteBufferPage[] allPages;
    private final int chunkSize;
   // private int prevAllocatedPage = 0;
    private AtomicInteger prevAllocatedPage;
    private final  int pageSize;
    private final short pageCount;
    private final int conReadBuferChunk ;
    /**
     * 记录对线程ID->该线程的所使用Direct Buffer的size
     */
    private final ConcurrentHashMap<Long,Long> memoryUsage;

    public DirectByteBufferPool(int pageSize, short chunkSize, short pageCount,int conReadBuferChunk) {
        allPages = new ByteBufferPage[pageCount];
        this.chunkSize = chunkSize;
        this.pageSize = pageSize;
        this.pageCount = pageCount;
        this.conReadBuferChunk = conReadBuferChunk;
        prevAllocatedPage = new AtomicInteger(0);
        for (int i = 0; i < pageCount; i++) {
            allPages[i] = new ByteBufferPage(ByteBuffer.allocateDirect(pageSize), chunkSize);
        }
        memoryUsage = new ConcurrentHashMap<>();
    }

    public BufferArray allocateArray() {
        return new BufferArray(this);
    }
    /**
     * TODO 当页不够时，考虑扩展内存池的页的数量...........
     * @param buffer
     * @return
     */
    public  ByteBuffer expandBuffer(ByteBuffer buffer){
        int oldCapacity = buffer.capacity();
        int newCapacity = oldCapacity << 1;
        ByteBuffer newBuffer = allocate(newCapacity);
        if(newBuffer != null){
            int newPosition = buffer.position();
            buffer.flip();
            newBuffer.put(buffer);
            newBuffer.position(newPosition);
            recycle(buffer);
            return  newBuffer;
        }
        return null;
    }

    public ByteBuffer allocate(int size) {
       final int theChunkCount = size / chunkSize + (size % chunkSize == 0 ? 0 : 1);
        int selectedPage =  prevAllocatedPage.incrementAndGet() % allPages.length;
        ByteBuffer byteBuf = allocateBuffer(theChunkCount, 0, selectedPage);
        if (byteBuf == null) {
            byteBuf = allocateBuffer(theChunkCount, selectedPage, allPages.length);
        }
        final long threadId = Thread.currentThread().getId();

        if(byteBuf !=null){
            if (memoryUsage.containsKey(threadId)){
                memoryUsage.put(threadId,memoryUsage.get(threadId)+byteBuf.capacity());
            }else {
                memoryUsage.put(threadId,(long)byteBuf.capacity());
            }
        }
        return byteBuf;
    }

    public void recycle(ByteBuffer theBuf) {
    	if(!(theBuf instanceof DirectBuffer)){
    		theBuf.clear();
    		return;
    	}

    	final long size = theBuf.capacity();

        boolean recycled = false;
        DirectBuffer thisNavBuf = (DirectBuffer) theBuf;
        int chunkCount = theBuf.capacity() / chunkSize;
        DirectBuffer parentBuf = (DirectBuffer) thisNavBuf.attachment();
        int startChunk = (int) ((thisNavBuf.address() - parentBuf.address()) / this.chunkSize);
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

    private ByteBuffer allocateBuffer(int theChunkCount, int startPage, int endPage) {
        for (int i = startPage; i < endPage; i++) {
            ByteBuffer buffer = allPages[i].allocatChunk(theChunkCount);
            if (buffer != null) {
                prevAllocatedPage.getAndSet(i);
                return buffer;
            }
        }
        return null;
    }

    public int getChunkSize() {
        return chunkSize;
    }
	
	 @Override
    public ConcurrentHashMap<Long,Long> getNetDirectMemoryUsage() {
        return memoryUsage;
    }

    public int getPageSize() {
        return pageSize;
    }

    public short getPageCount() {
        return pageCount;
    }

    //TODO   should  fix it
    public long capacity(){
        return size();
    }

    public long size(){
        return  (long) pageSize * chunkSize * pageCount;
    }

    //TODO
    public  int getSharedOptsCount(){
        return 0;
    }


    public int getConReadBuferChunk() {
        return conReadBuferChunk;
    }

}
