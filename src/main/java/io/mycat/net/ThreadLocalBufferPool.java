package io.mycat.net;


public class ThreadLocalBufferPool extends ThreadLocal<BufferQueue> {
	private final long size;

	public ThreadLocalBufferPool(long size) {
		this.size = size;
	}
	
	@Override
	protected synchronized BufferQueue initialValue() {
		return new BufferQueue(size);
	}
}
