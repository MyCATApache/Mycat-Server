package io.mycat.net;

import io.mycat.net.BufferQueue;

public class ThreadLocalBufferPool extends ThreadLocal<BufferQueue> {
	private final int size;

	public ThreadLocalBufferPool(int size) {
		this.size = size;
	}

	protected synchronized BufferQueue initialValue() {
		return new BufferQueue(size);
	}
}
