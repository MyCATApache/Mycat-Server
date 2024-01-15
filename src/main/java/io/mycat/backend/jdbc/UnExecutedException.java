package io.mycat.backend.jdbc;

public class UnExecutedException extends Exception {
    public UnExecutedException() {}
    public UnExecutedException(String message) {
        super(message);
    }

    public UnExecutedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnExecutedException(Throwable cause) {
        super(cause);
    }
}
