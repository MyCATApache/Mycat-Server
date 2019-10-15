package io.mycat.buffer;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import sun.nio.ch.DirectBuffer;

/*
 * 用来保存一个一个ByteBuffer为底层存储的内存页
 */
@SuppressWarnings("restriction")
public class ByteBufferPage {

    private final ByteBuffer buf;
    private final int chunkSize;    //chunk的大小 一般为4k
    private final int chunkCount;  //chunk的个数
    private final BitSet chunkAllocateTrack; //某个chunk是否被分配
    private final AtomicBoolean allocLockStatus = new AtomicBoolean(false);  //锁
    private final long startAddress;
    private final ConcurrentHashMap<Long, Long> relationBufferThreadId;

    public ByteBufferPage(ByteBuffer buf, int chunkSize) {
        super();
        this.chunkSize = chunkSize;
        chunkCount = buf.capacity() / chunkSize;
        chunkAllocateTrack = new BitSet(chunkCount);
        relationBufferThreadId = new ConcurrentHashMap<>(chunkCount);
        this.buf = buf;
        startAddress = ((sun.nio.ch.DirectBuffer) buf).address();
    }

    public ByteBuffer allocatChunk(int theChunkCount) {
    	//加锁 成功执行不成功返回
        if (!allocLockStatus.compareAndSet(false, true)) {
            return null;
        }
        int startChunk = -1;//从startChunk开始的
        int contiueCount = 0;//连续的chunk个数
        try {
        	///枚举寻找连续的N个chunk
            for (int i = 0; i < chunkCount; i++) {
            	//找到一个可用的
                if (chunkAllocateTrack.get(i) == false) {
                	//如果是第一个 则设置startChunk
                	if (startChunk == -1) {
                    	//从头开始找
                        startChunk = i;
                        contiueCount = 1; 
                        if (theChunkCount == 1) {
                            break;
                        }
                    } else {
                    	//连续chunk个数加一 ,是否找到连续的chunk个数,是则返回.
                        if (++contiueCount == theChunkCount) {
                            break;
                        }
                    }
                } else {
                	//不连续了
                    startChunk = -1;
                    contiueCount = 0;
                }
            }
            //找到了 
            if (contiueCount == theChunkCount) {
                int offStart = startChunk * chunkSize;
                int offEnd = offStart + theChunkCount * chunkSize;
                buf.limit(offEnd);
                buf.position(offStart);

                ByteBuffer newBuf = buf.slice(); //分配buffer
                //sun.nio.ch.DirectBuffer theBuf = (DirectBuffer) newBuf;
                //System.out.println("offAddress " + (theBuf.address() - startAddress));
                //设置chunk为已用
                markChunksUsed(startChunk, theChunkCount);
                relationBufferThreadId.put(((DirectBuffer) newBuf).address(), Thread.currentThread().getId());

                return newBuf;
            } else {
                //System.out.println("contiueCount " + contiueCount + " theChunkCount " + theChunkCount);
                return null;
            }
        } finally {
            allocLockStatus.set(false);
        }
    }
    //设置已用
    private void markChunksUsed(int startChunk, int theChunkCount) {
        for (int i = 0; i < theChunkCount; i++) {
            chunkAllocateTrack.set(startChunk + i);
        }
    }
    //清空不可用
    private void markChunksUnused(int startChunk, int theChunkCount) {
        for (int i = 0; i < theChunkCount; i++) {
            chunkAllocateTrack.clear(startChunk + i);
        }
    }

    /**
     * 回收buffer
     * @param parent      当前要释放的buf的parent 
     * @param recycleBuf  当前要释放的recycleBuf
     * @param startChunk
     * @param chunkCount
     * @param relatedThreadId  用于返回当前要释放的recycleBuf关联的线程id
     * @return
     */
    public boolean recycleBuffer(ByteBuffer parent, ByteBuffer recycleBuf, int startChunk, int chunkCount,
            StringBuilder relatedThreadId) {

        if (parent == this.buf) {
        	//获取锁 必须获取成功
            while (!this.allocLockStatus.compareAndSet(false, true)) {
                Thread.yield();
            }
            //清空已用状态
            try {
                markChunksUnused(startChunk,chunkCount);
                Long threadId = relationBufferThreadId.remove(((DirectBuffer) recycleBuf).address());
                if (threadId != null) {
                    relatedThreadId.append(threadId);
                }
            } finally {
            	//释放锁
                allocLockStatus.set(false);
            }
            return true;
        }
        return false;
    }
}
