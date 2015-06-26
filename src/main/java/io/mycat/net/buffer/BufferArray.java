package io.mycat.net.buffer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * used for large data write ,composed by buffer array, when a large MySQL
 * package write ,shoud use this object to write data
 * 
 * @author wuzhih
 * 
 */
public class BufferArray {

	private final BufferPool bufferPool;
	private ByteBuffer curWritingBlock;
	private List<ByteBuffer> writedBlockLst = Collections.emptyList();

	public BufferArray(BufferPool bufferPool) {
		super();
		this.bufferPool = bufferPool;
		curWritingBlock = bufferPool.allocate();
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
				curWritingBlock = bufferPool.allocate();
				continue;
			}

		}
		return curWritingBlock;
	}

}
