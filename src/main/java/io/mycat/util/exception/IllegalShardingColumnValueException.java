package io.mycat.util.exception;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/7/26
 */
public class IllegalShardingColumnValueException extends Exception {

    public IllegalShardingColumnValueException() {
        super();
    }

    public IllegalShardingColumnValueException(String message) {
        super(message);
    }

    public IllegalShardingColumnValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public IllegalShardingColumnValueException(Throwable cause) {
        super(cause);
    }

    @Override
    public Throwable fillInStackTrace()
    {
        return this;
    }
}
