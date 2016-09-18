package io.mycat.memory.unsafe.ringbuffer.exception;

/**
 * @author lmax.Disruptor
 * @version 3.3.5
 * @date 2016/7/24
 */
public class AlertException extends Exception {

    public static final AlertException INSTANCE = new AlertException();

    private AlertException()
    {
    }

    @Override
    public Throwable fillInStackTrace()
    {
        return this;
    }
}
