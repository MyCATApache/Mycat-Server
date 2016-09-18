package io.mycat.util.exception;
/**
 * 
 * @author haonan108
 *
 */
public class DataMigratorException  extends RuntimeException{

	private static final long serialVersionUID = -6706826479467595980L;

	public DataMigratorException() {
		super();
		
	}

	public DataMigratorException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		
	}

	public DataMigratorException(String message, Throwable cause) {
		super(message, cause);
		
	}

	public DataMigratorException(String message) {
		super(message);
		
	}

	public DataMigratorException(Throwable cause) {
		super(cause);
		
	}
}
