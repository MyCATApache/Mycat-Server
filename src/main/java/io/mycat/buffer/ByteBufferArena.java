package io.mycat.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 仿照Netty的思路，针对MyCat内存缓冲策略优化
 * ByteBufferArena维护着锁还有所有list
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 17:19 2016/5/17
 * @see @https://github.com/netty/netty
 */
public class ByteBufferArena implements BufferPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferChunkList.class);
    private final ByteBufferChunkList q[];

    private final AtomicInteger chunkCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);

    private final int FAIL_THRESHOLD = 1000;
    private final int pageSize;
    private final int chunkSize;

    private final AtomicInteger capacity;
    private final AtomicInteger size;

    private final ConcurrentHashMap<Thread, Integer> sharedOptsCount;

    private final int conReadBuferChunk;

    public ByteBufferArena(int chunkSize, int pageSize, int chunkCount, int conReadBuferChunk) {
        try {
            this.chunkSize = chunkSize;
            this.pageSize = pageSize;
            this.chunkCount.set(chunkCount);
            this.conReadBuferChunk = conReadBuferChunk;

            q = new ByteBufferChunkList[6];
            q[5] = new ByteBufferChunkList(100, Integer.MAX_VALUE, chunkSize, pageSize, 0);
            q[4] = new ByteBufferChunkList(75, 100, chunkSize, pageSize, 0);
            q[3] = new ByteBufferChunkList(50, 100, chunkSize, pageSize, 0);
            q[2] = new ByteBufferChunkList(25, 75, chunkSize, pageSize, 0);
            q[1] = new ByteBufferChunkList(1, 50, chunkSize, pageSize, 0);
            q[0] = new ByteBufferChunkList(Integer.MIN_VALUE, 25, chunkSize, pageSize, chunkCount);

            q[0].nextList = q[1];
            q[1].nextList = q[2];
            q[2].nextList = q[3];
            q[3].nextList = q[4];
            q[4].nextList = q[5];
            q[5].nextList = null;

            q[5].prevList = q[4];
            q[4].prevList = q[3];
            q[3].prevList = q[2];
            q[2].prevList = q[1];
            q[1].prevList = q[0];
            q[0].prevList = null;

            capacity = new AtomicInteger(6 * chunkCount * chunkSize);
            size = new AtomicInteger(6 * chunkCount * chunkSize);
            sharedOptsCount = new ConcurrentHashMap<>();
        } finally {
        }
    }

    @Override
    public ByteBuffer allocate(int reqCapacity) {
        try {
            ByteBuffer byteBuffer = null;
            int i = 0, count = 0;
            while (byteBuffer == null) {
                if (i > 5) {
                    i = 0;
                    count = failCount.incrementAndGet();
                    if (count > FAIL_THRESHOLD) {
                        try {
                            expand();
                        } finally {
                        }
                    }
                }
                byteBuffer = q[i].allocate(reqCapacity);
                i++;
            }
//            if (count > 0) {
//                System.out.println("count: " + count);
//                System.out.println(failCount.get());
//            }
//            printList();
            capacity.addAndGet(-reqCapacity);
            final Thread thread =  Thread.currentThread();
            if (sharedOptsCount.contains(thread)) {
                int currentCount = sharedOptsCount.get(thread);
                currentCount++;
                sharedOptsCount.put(thread,currentCount);
            } else{
                sharedOptsCount.put(thread,0);
            }
            return byteBuffer;
        } finally {
        }
    }

    private void expand() {
        LOGGER.warn("Current Buffer Size is not enough! Expanding Byte buffer!");
        ByteBufferChunk byteBufferChunk = new ByteBufferChunk(pageSize, chunkSize);
        q[0].byteBufferChunks.add(byteBufferChunk);
        failCount.set(0);
    }

    @Override
    public void recycle(ByteBuffer byteBuffer) {
        try {
            int i;
            for (i = 0; i < 6; i++) {
                if (q[i].free(byteBuffer)) {
                    break;
                }
            }
            if (i > 5) {
                LOGGER.warn("This ByteBuffer is not maintained in ByteBufferArena!");
                return;
            }
            final Thread thread =  Thread.currentThread();
            if (sharedOptsCount.contains(thread)) {
                int currentCount = sharedOptsCount.get(thread);
                currentCount--;
                sharedOptsCount.put(thread,currentCount);
            } else{
                sharedOptsCount.put(thread,0);
            }
            capacity.addAndGet(byteBuffer.capacity());
            return;
        } finally {
        }
    }

    private void printList() {
        for (int i = 0; i < 6; i++) {
            System.out.println(i + ":" + q[i].byteBufferChunks.toString());
        }
    }

    @Override
    public int capacity() {
        return capacity.get();
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public int getConReadBuferChunk() {
        return conReadBuferChunk;
    }

    @Override
    public int getSharedOptsCount() {
        final Set<Integer> integers = (Set<Integer>) sharedOptsCount.values();
        int count = 0;
        for(int i : integers){
            count += i;
        }
        return count;
    }

    /**
     * 这里pageSize就是DirectByteBuffer的chunksize
     * @return
     */
    @Override
    public int getChunkSize() {
        return pageSize;
    }

    @Override
    public BufferArray allocateArray() {
        return new BufferArray(this);
    }
}
