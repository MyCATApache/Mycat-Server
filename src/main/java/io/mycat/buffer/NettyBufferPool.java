package io.mycat.buffer;


import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 封装netty pooled Direct Memory 接口，为mycat提供内存分配功能
 * 由于Mycat目前使用ByteBuffer,而Netty分配的是ByteBuf，为了管理ByteBuf
 * 在MyCatMemoryAllocator中定义recycleMaps ByteBuffer(address) -->ByteBuf
 * 的映射关系，通过address来回收ByteBuf.
 *
 * @author zagnix
 * @create 2017-04-13
 */

public class NettyBufferPool implements BufferPool {
    MyCatMemoryAllocator allocator;
    private int chunkSize = 0;

    public NettyBufferPool(int chunkSize) {
        allocator = MyCatMemoryAllocator.getINSTANCE();
        this.chunkSize = chunkSize;
    }

    @Override
    public ByteBuffer allocate(int size) {
        ByteBuf byteBuf = allocator.directBuffer(size);
        ByteBuffer byteBuffer = byteBuf.nioBuffer(0, size);
        allocator.recycleMaps.put(PlatformDependent.directBufferAddress(byteBuffer), byteBuf);
        return byteBuffer;
    }

    @Override
    public void recycle(ByteBuffer byteBuffer) {
        ByteBuf byteBuf =
                allocator.recycleMaps.get(PlatformDependent.directBufferAddress(byteBuffer));
        if (byteBuf != null) {
            byteBuf.release();
        }
        allocator.recycleMaps.remove(PlatformDependent.directBufferAddress(byteBuffer));
    }

    /**
     * TODO
     * 下面函数需要将netty相关内存信息导出处理，然后实现
     */
    @Override
    public long capacity() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public int getConReadBuferChunk() {
        return 0;
    }

    @Override
    public int getSharedOptsCount() {
        return 0;
    }

    @Override
    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public ConcurrentHashMap<Long, Long> getNetDirectMemoryUsage() {
        return null;
    }

    @Override
    public BufferArray allocateArray() {
        return new BufferArray(this);
    }
}
