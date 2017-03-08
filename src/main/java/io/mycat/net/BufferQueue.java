package io.mycat.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author wuzh
 */
public final class BufferQueue {
	private final long total;
	private final LinkedList<ByteBuffer> items = new LinkedList<ByteBuffer>();

	public BufferQueue(long capacity) {
		this.total = capacity;
	}

	/**
	 * used for statics
	 * 
	 * @return
	 */
	public long snapshotSize() {
		return this.items.size();
	}

	public Collection<ByteBuffer> removeItems(long count) {

		ArrayList<ByteBuffer> removed = new ArrayList<ByteBuffer>();
		Iterator<ByteBuffer> itor = items.iterator();
		while (itor.hasNext()) {
			removed.add(itor.next());
			itor.remove();
			if (removed.size() >= count) {
				break;
			}
		}
		return removed;
	}

	/**
	 * 
	 * @param buffer
	 * @throws InterruptedException
	 */
	public void put(ByteBuffer buffer) {
		this.items.offer(buffer);
		if (items.size() > total) {
			throw new java.lang.RuntimeException(
					"bufferQueue size exceeded ,maybe sql returned too many records ,cursize:"
							+ items.size());

		}
	}

	public ByteBuffer poll() {
		return items.poll();
	}

	public boolean isEmpty() {
		return items.isEmpty();
	}

}