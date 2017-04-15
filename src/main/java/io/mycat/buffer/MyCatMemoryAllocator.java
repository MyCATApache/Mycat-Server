package io.mycat.buffer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * Netty Direct Memory 分配器，为mycat提供内存池管理功能
 *
 * @author zagnix
 * @create 2017-01-18 11:01
 */

public class MyCatMemoryAllocator implements ByteBufAllocator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyCatMemoryAllocator.class);
    public final ConcurrentHashMap<Long,ByteBuf> recycleMaps = new ConcurrentHashMap<>();

    private final static MyCatMemoryAllocator INSTANCE =
           new MyCatMemoryAllocator(Runtime.getRuntime().availableProcessors());

    /** netty memory pool alloctor*/
    private final PooledByteBufAllocator alloc;
    /**arena 的数量，一般设置cpu cores*2 */
    private final int numberOfArenas;

    /** ChunkSize 大小 = pageSize << maxOrder */
    private final int chunkSize;

    /**
     * numberOfArenas 设置为处理器cores*2
     * @param numberOfArenas
     */
    public MyCatMemoryAllocator(int numberOfArenas){
        this.numberOfArenas = numberOfArenas;
        if (!PlatformDependent.hasUnsafe()) {
           LOGGER.warn("Using direct memory, but sun.misc.Unsafe not available.");
        }
        boolean preferDirect = true;

        int pageSize = 8192*2;
        int maxOrder = 11;
        this.chunkSize = pageSize << maxOrder;
        int numDirectArenas = numberOfArenas;
        int numHeapArenas = 0;

        /** for 4.1.x*/
        this.alloc = new PooledByteBufAllocator(
                preferDirect,
                numHeapArenas,
                numDirectArenas,
                pageSize,
                maxOrder,
                512,
                256,
                64,
                true);


        /**for 5.0.x
        this.alloc = new PooledByteBufAllocator(preferDirect);**/
    }

    public static MyCatMemoryAllocator getINSTANCE() {
        return INSTANCE;
    }

    /**
     * @return alloc
     */
    public PooledByteBufAllocator getAlloc() {
        return alloc;
    }

    /**
     * Returns the number of arenas.
     *
     * @return Number of arenas.
     */
    int getNumberOfArenas() {
        return numberOfArenas;
    }

    /**
     * Returns the chunk size.
     *
     * @return Chunk size.
     */
    int getChunkSize() {
        return chunkSize;
    }


    @Override
    public ByteBuf buffer() {
        return alloc.buffer();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return alloc.buffer(initialCapacity);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return alloc.buffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf ioBuffer() {
        return alloc.ioBuffer();
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        return alloc.ioBuffer(initialCapacity);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return alloc.ioBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf heapBuffer() {
        throw new UnsupportedOperationException("Heap buffer");
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        throw new UnsupportedOperationException("Heap buffer");
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        throw new UnsupportedOperationException("Heap buffer");
    }

    @Override
    public ByteBuf directBuffer() {
        return alloc.directBuffer();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return alloc.directBuffer(initialCapacity);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        return alloc.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return alloc.compositeBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return alloc.compositeBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        throw new UnsupportedOperationException("Heap buffer");
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        throw new UnsupportedOperationException("Heap buffer");
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return alloc.compositeDirectBuffer();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return alloc.compositeDirectBuffer(maxNumComponents);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return alloc.isDirectBufferPooled();
    }

    @Override
    public int calculateNewCapacity(int i, int i1) {
        return 0;
    }
}
