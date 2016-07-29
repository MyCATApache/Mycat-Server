package io.mycat.memory.unsafe.ringbuffer;


import com.lmax.disruptor.EventSequencer;
import com.lmax.disruptor.InsufficientCapacityException;
import io.mycat.memory.unsafe.Platform;
import io.mycat.memory.unsafe.array.LongArray;
import io.mycat.memory.unsafe.memory.mm.DataNodeMemoryManager;
import io.mycat.memory.unsafe.memory.mm.MemoryConsumer;
import io.mycat.memory.unsafe.ringbuffer.common.Cursored;
import io.mycat.memory.unsafe.ringbuffer.common.event.*;
import io.mycat.memory.unsafe.ringbuffer.producer.Sequencer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 环形buffer 待实现，
 */
public class RingBuffer<E> extends MemoryConsumer implements Cursored, EventSequencer<E>, EventSink<E> {
    //Buffer数组填充
    private static final int BUFFER_PAD;
    //Buffer数组起始基址
    private static final long REF_ARRAY_BASE;
    //2^n=每个数组对象引用所占空间，这个n就是REF_ELEMENT_SHIFT
    private static final int REF_ELEMENT_SHIFT;

    static {
        final int scale = Platform.arrayIndexScale(Object[].class);
        //Object数组引用长度，32位为4字节，64位为8字节
        if (4 == scale) {
            REF_ELEMENT_SHIFT = 2;
        } else if (8 == scale) {
            REF_ELEMENT_SHIFT = 3;
        } else {
            throw new IllegalStateException("Unknown pointer size");
        }
        //需要填充128字节，缓存行长度一般是128字节
        BUFFER_PAD = 128 / scale;
        REF_ARRAY_BASE = Platform.arrayBaseOffset(Object[].class) + (BUFFER_PAD << REF_ELEMENT_SHIFT);
    }

    private final long indexMask;
    private final Object[] entries;
    protected final int bufferSize;
    protected final Sequencer sequencer;

    public RingBuffer(DataNodeMemoryManager dataNodeMemoryManager, EventFactory<E> eventFactory, Sequencer sequencer) {
        super(dataNodeMemoryManager);
        this.sequencer = sequencer;
        this.bufferSize = sequencer.getBufferSize();
        //保证buffer大小不小于1
        if (bufferSize < 1) {
            throw new IllegalArgumentException("bufferSize must not be less than 1");
        }
        //保证buffer大小为2的n次方
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        //m % 2^n  <=>  m & (2^n - 1)
        this.indexMask = bufferSize - 1;
        /**
         * 结构：缓存行填充，避免频繁访问的任一entry与另一被修改的无关变量写入同一缓存行
         * --------------
         * *   数组头   * BASE
         * *   Padding  * 128字节
         * * reference1 * SCALE
         * * reference2 * SCALE
         * * reference3 * SCALE
         * ..........
         * *   Padding  * 128字节
         * --------------
         */
        this.entries = new Object[sequencer.getBufferSize() + 2 * BUFFER_PAD];
        //利用eventFactory初始化RingBuffer的每个槽
        fill(eventFactory);
    }

    private void fill(EventFactory<E> eventFactory) {
        for (int i = 0; i < bufferSize; i++) {
            entries[BUFFER_PAD + i] = eventFactory.newInstance();
        }
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        return 0;
    }

    @Override
    public long getCursor() {
        return 0;
    }

    @Override
    public E get(long sequence) {
        return null;
    }

    @Override
    public void publishEvent(EventTranslator<E> translator) {

    }

    @Override
    public boolean tryPublishEvent(EventTranslator<E> translator) {
        return false;
    }

    @Override
    public <A> void publishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {

    }

    @Override
    public <A> boolean tryPublishEvent(EventTranslatorOneArg<E, A> translator, A arg0) {
        return false;
    }

    @Override
    public <A, B> void publishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {

    }

    @Override
    public <A, B> boolean tryPublishEvent(EventTranslatorTwoArg<E, A, B> translator, A arg0, B arg1) {
        return false;
    }

    @Override
    public <A, B, C> void publishEvent(EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2) {

    }

    @Override
    public <A, B, C> boolean tryPublishEvent(EventTranslatorThreeArg<E, A, B, C> translator, A arg0, B arg1, C arg2) {
        return false;
    }

    @Override
    public void publishEvent(EventTranslatorVararg<E> translator, Object... args) {

    }

    @Override
    public boolean tryPublishEvent(EventTranslatorVararg<E> translator, Object... args) {
        return false;
    }

    @Override
    public void publishEvents(EventTranslator<E>[] translators) {

    }

    @Override
    public void publishEvents(EventTranslator<E>[] translators, int batchStartsAt, int batchSize) {

    }

    @Override
    public boolean tryPublishEvents(EventTranslator<E>[] translators) {
        return false;
    }

    @Override
    public boolean tryPublishEvents(EventTranslator<E>[] translators, int batchStartsAt, int batchSize) {
        return false;
    }

    @Override
    public <A> void publishEvents(EventTranslatorOneArg<E, A> translator, A[] arg0) {

    }

    @Override
    public <A> void publishEvents(EventTranslatorOneArg<E, A> translator, int batchStartsAt, int batchSize, A[] arg0) {

    }

    @Override
    public <A> boolean tryPublishEvents(EventTranslatorOneArg<E, A> translator, A[] arg0) {
        return false;
    }

    @Override
    public <A> boolean tryPublishEvents(EventTranslatorOneArg<E, A> translator, int batchStartsAt, int batchSize, A[] arg0) {
        return false;
    }

    @Override
    public <A, B> void publishEvents(EventTranslatorTwoArg<E, A, B> translator, A[] arg0, B[] arg1) {

    }

    @Override
    public <A, B> void publishEvents(EventTranslatorTwoArg<E, A, B> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1) {

    }

    @Override
    public <A, B> boolean tryPublishEvents(EventTranslatorTwoArg<E, A, B> translator, A[] arg0, B[] arg1) {
        return false;
    }

    @Override
    public <A, B> boolean tryPublishEvents(EventTranslatorTwoArg<E, A, B> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1) {
        return false;
    }

    @Override
    public <A, B, C> void publishEvents(EventTranslatorThreeArg<E, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2) {

    }

    @Override
    public <A, B, C> void publishEvents(EventTranslatorThreeArg<E, A, B, C> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1, C[] arg2) {

    }

    @Override
    public <A, B, C> boolean tryPublishEvents(EventTranslatorThreeArg<E, A, B, C> translator, A[] arg0, B[] arg1, C[] arg2) {
        return false;
    }

    @Override
    public <A, B, C> boolean tryPublishEvents(EventTranslatorThreeArg<E, A, B, C> translator, int batchStartsAt, int batchSize, A[] arg0, B[] arg1, C[] arg2) {
        return false;
    }

    @Override
    public void publishEvents(EventTranslatorVararg<E> translator, Object[]... args) {

    }

    @Override
    public void publishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args) {

    }

    @Override
    public boolean tryPublishEvents(EventTranslatorVararg<E> translator, Object[]... args) {
        return false;
    }

    @Override
    public boolean tryPublishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args) {
        return false;
    }

    @Override
    public int getBufferSize() {
        return 0;
    }

    @Override
    public boolean hasAvailableCapacity(int requiredCapacity) {
        return false;
    }

    @Override
    public long remainingCapacity() {
        return 0;
    }

    @Override
    public long next() {
        return 0;
    }

    @Override
    public long next(int n) {
        return 0;
    }

    @Override
    public long tryNext() throws InsufficientCapacityException {
        return 0;
    }

    @Override
    public long tryNext(int n) throws InsufficientCapacityException {
        return 0;
    }

    @Override
    public void publish(long sequence) {

    }

    @Override
    public void publish(long lo, long hi) {

    }
}
