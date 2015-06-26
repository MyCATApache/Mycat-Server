package io.mycat.net;

public class ConnectionException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final int code;
	private final String msg;

	public ConnectionException(int code, String msg) {
		super();
		this.code = code;
		this.msg = msg;
	}

	@Override
	public String toString() {
		return "ConnectionException [code=" + code + ", msg=" + msg + "]";
	}

}