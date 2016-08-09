package io.mycat.memory.unsafe.ringbuffer.common.event;

/**
 * Event初始化接口，生产者通过实现这个接口，在发布Event时，对应实现的translateTo方法会被调用
 * 这里用户可以传多个参数
 *
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/29
 */
public interface EventTranslatorVararg<T> {
     void translateTo(final T event, long sequence, final Object... args);
}
