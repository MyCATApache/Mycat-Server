package org.opencloudb.sqlengine;


public interface SQLQueryResultListener<T> {

	public void onResult(T result);

}
