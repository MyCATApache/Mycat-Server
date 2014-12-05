package org.opencloudb.exception;


/**
 * thrown in public RouteResultset RouteService.route(SystemConfig sysconf, SchemaConfig schema,
			int sqlType, String stmt, String charset, ServerConnection sc)
			throws SQLNonTransientException 
	if comment in stmt does not meet / *!mycat: type = value * /
 * @author runfriends@126.com
 *
 */
public class ShardingCommentException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8524382590800902425L;

	public ShardingCommentException() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ShardingCommentException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public ShardingCommentException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public ShardingCommentException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public ShardingCommentException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

}
