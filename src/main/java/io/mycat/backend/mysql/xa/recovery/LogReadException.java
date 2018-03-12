package io.mycat.backend.mysql.xa.recovery;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class LogReadException extends LogException{

    private static final long serialVersionUID = -4835268355879075429L;

    public LogReadException() {
        super();
    }

    public LogReadException(Throwable cause) {
        super(cause);
    }

    public LogReadException(String message) {
        super(message);
    }

}
