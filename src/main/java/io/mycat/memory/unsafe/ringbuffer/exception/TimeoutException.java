package io.mycat.memory.unsafe.ringbuffer.exception;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/24
 */
public class TimeoutException extends Exception {
    public static final TimeoutException INSTANCE = new TimeoutException();

    private TimeoutException()
    {
    }

    @Override
    public synchronized Throwable fillInStackTrace()
    {
        return this;
    }
}
