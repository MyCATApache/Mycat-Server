package io.mycat.buffer;

import java.nio.ByteBuffer;

/**
 * 缓冲池
 *
 * @author Hash Zhang
 * @version 1.0
 * @time 12:19 2016/5/23
 */
public interface BufferPool {
    public ByteBuffer allocate(int size);
    public void recycle(ByteBuffer theBuf);
    public int capacity();
    public int size();
    public int getConReadBuferChunk();
    public  int getSharedOptsCount();
    public int getChunkSize();
    public BufferArray allocateArray();
}
