package org.opencloudb.sqlengine;

public class SQLQueryResult<T> {
private final T result;
private final boolean success;

public SQLQueryResult(T result, boolean success) {
	super();
	this.result = result;
	this.success = success;
}
public T getResult() {
	return result;
}

public boolean isSuccess() {
	return success;
}


}
