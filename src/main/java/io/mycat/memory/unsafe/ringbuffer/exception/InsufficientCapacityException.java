package io.mycat.memory.unsafe.ringbuffer.exception;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/23
 */
@SuppressWarnings("serial")
public final class InsufficientCapacityException extends Exception {
    public static final InsufficientCapacityException INSTANCE = new InsufficientCapacityException();

    private InsufficientCapacityException() {
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
