package io.mycat.memory.unsafe.ringbuffer.common.event;

import io.mycat.memory.unsafe.ringbuffer.common.DataProvider;
import io.mycat.memory.unsafe.ringbuffer.common.Sequenced;

/**
 * EventSequencer接口没有自己的方法，只是为了将Sequencer和DataProvider合起来。
 *
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/29
 */
public interface EventSequencer<T> extends DataProvider<T>, Sequenced {
}
