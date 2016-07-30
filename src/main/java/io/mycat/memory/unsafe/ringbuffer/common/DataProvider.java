package io.mycat.memory.unsafe.ringbuffer.common;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/29
 */
public interface DataProvider<T> {
    /**
     * 获取sequence对应的对象
     * @param sequence
     * @return
     */
    public T get(long sequence);
}
