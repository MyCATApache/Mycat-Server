package org.opencloudb.sqlengine;


public interface SQLQueryResultListener<T> {

	public void onRestult(T result);

}
