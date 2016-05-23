package io.mycat.buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 仿照Netty的思路，针对MyCat内存缓冲策略优化
 * ChunkList维护着一个指向一串Chunk的头结点，访问策略由minUsage，maxUsage决定
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 17:19 2016/5/17
 * @see @https://github.com/netty/netty
 */
public class ByteBufferChunkList {
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBufferChunkList.class);
    private final int minUsage;
    private final int maxUsage;

    Set<ByteBufferChunk> byteBufferChunks;
    ByteBufferChunkList prevList;
    ByteBufferChunkList nextList;

    public ByteBufferChunkList(int minUsage, int maxUsage, int chunkSize, int pageSize, int numOfChunks) {
        this.minUsage = minUsage;
        this.maxUsage = maxUsage;
        byteBufferChunks = new ConcurrentSkipListSet<>();
        for (int i = 0; i < numOfChunks; i++) {
            ByteBufferChunk chunk = new ByteBufferChunk(pageSize, chunkSize);
            byteBufferChunks.add(chunk);
        }
    }

    public ByteBufferChunk getIndex(ByteBuffer buffer) {
        for(ByteBufferChunk byteBufferChunk : byteBufferChunks){
            if (byteBufferChunk.isInThisChunk(buffer))
                return byteBufferChunk;
        }
        return null;
    }

    ByteBuffer allocate(int reqCapacity) {
        for (ByteBufferChunk cur : byteBufferChunks) {
            ByteBuffer buf = cur.allocateRun(reqCapacity);
            if (buf == null) {
                continue;
            } else {
                final int usage = cur.usage();
                if (usage >= maxUsage) {
                    ByteBufferChunkList next = nextList;
                    ByteBufferChunkList current = this;
                    while (next != null) {
                        current.byteBufferChunks.remove(cur);
                        next.byteBufferChunks.add(cur);
                        if (next.maxUsage > usage) {
                            break;
                        }
                        current = next;
                        next = next.nextList;
                    }
                }
                return buf;
            }
        }
        return null;
    }

    boolean free(ByteBuffer buffer) {
        ByteBufferChunk cur = getIndex(buffer);
        if (cur == null) {
            LOGGER.info("not in this list!");
            return false;
        }
        cur.freeByteBuffer(buffer);
        final int usage = cur.usage();
        if (usage < minUsage) {
            ByteBufferChunkList prev = prevList;
            ByteBufferChunkList current = this;
            while (prev != null) {
                current.byteBufferChunks.remove(cur);
                prev.byteBufferChunks.add(cur);
                if (prev.minUsage < usage) {
                    break;
                }
                current = prev;
                prev = prev.prevList;
            }
        }
        return true;
    }
}
