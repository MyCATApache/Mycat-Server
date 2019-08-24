package io.mycat.memory.unsafe.ringbuffer.common.event;

/**
 * 用户实现，生成Event的接口
 *
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/29
 */
public interface EventFactory<T> {
    T newInstance();
}
