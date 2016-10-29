package io.mycat.backend.mysql.xa.recovery;

/**
 * Created by zhangchao on 2016/10/13.
 */
public class LogException extends Exception{
    private static final long serialVersionUID = 3259337218182873867L;

    public LogException() {
        super();
    }

    public LogException(String message) {
        super(message);
    }

    public LogException(Throwable cause) {
        super(cause);
    }
}
