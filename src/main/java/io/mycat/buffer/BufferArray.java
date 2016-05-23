package io.mycat.buffer;

import io.mycat.util.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * used for large data write ,composed by buffer array, when a large MySQL
 * package write ,shoud use this object to write data
 *
 *  use DirectByteBuffer for alloc buffer
 * @author wuzhih
 * @author zagnix
 */
public class BufferArray {
	private final BufferPool bufferPool;
	private ByteBuffer curWritingBlock;
	private List<ByteBuffer> writedBlockLst = Collections.emptyList();

	public BufferArray(BufferPool bufferPool) {
		super();
		this.bufferPool = bufferPool;
		curWritingBlock = bufferPool.allocate(bufferPool.getChunkSize());
	}

	public ByteBuffer checkWriteBuffer(int capacity) {
		if (capacity > curWritingBlock.remaining()) {
			addtoBlock(curWritingBlock);
			curWritingBlock = bufferPool.allocate(capacity);
			return curWritingBlock;
		} else {
			return curWritingBlock;
		}
	}

	public int getBlockCount()
	{
		return writedBlockLst.size()+1;
	}
	private void addtoBlock(ByteBuffer buffer) {
		if (writedBlockLst.isEmpty()) {
			writedBlockLst = new LinkedList<ByteBuffer>();
		}
		writedBlockLst.add(buffer);
	}

	public ByteBuffer getCurWritingBlock() {
		return curWritingBlock;
	}

	public List<ByteBuffer> getWritedBlockLst() {
		return writedBlockLst;
	}

	public void clear() {
		curWritingBlock = null;
		writedBlockLst.clear();
		writedBlockLst = null;
	}

	public ByteBuffer write(byte[] src) {
		int offset = 0;
		int remains = src.length;
		while (remains > 0) {
			int writeable = curWritingBlock.remaining();
			if (writeable >= remains) {
				// can write whole srce
				curWritingBlock.put(src, offset, remains);
				break;
			} else {
				// can write partly
				curWritingBlock.put(src, offset, writeable);
				offset += writeable;
				remains -= writeable;
				addtoBlock(curWritingBlock);
				curWritingBlock = bufferPool.allocate(bufferPool.getChunkSize());
				continue;
			}
		}
		return curWritingBlock;
	}


    public byte[] writeToByteArrayAndRecycle() {
        BufferArray bufferArray=this;
        try {

              int size=0;
            List<ByteBuffer> blockes = bufferArray.getWritedBlockLst();
            if (!bufferArray.getWritedBlockLst().isEmpty()) {
                for (ByteBuffer curBuf : blockes) {
                    curBuf.flip();
                    size+=curBuf.remaining();
                }
            }
            ByteBuffer curBuf = bufferArray.getCurWritingBlock();
            curBuf.flip();
            if(curBuf.hasRemaining())
            {
                size += curBuf.remaining();
            }
            if(size>0)
            {
                int offset=0;
                byte[] all=new byte[size];
                if (!bufferArray.getWritedBlockLst().isEmpty()) {
                    for (ByteBuffer tBuf : blockes) {

                        ByteBufferUtil.arrayCopy(tBuf,0,all,offset,tBuf.remaining());
                        offset+=tBuf.remaining();

                        bufferPool.recycle(tBuf);
                    }
                }
                ByteBuffer tBuf = bufferArray.getCurWritingBlock();
                if(tBuf.hasRemaining())
                {
                    ByteBufferUtil.arrayCopy(tBuf,0,all,offset,tBuf.remaining());
                    bufferPool.recycle(tBuf);
                   // offset += curBuf.remaining();
                }
                return all;
            }

        } finally {

            bufferArray.clear();
        }

      return EMPTY;
    }

    private static byte[] EMPTY=new byte[0];

}
