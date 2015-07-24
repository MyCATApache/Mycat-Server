package io.mycat.sqlengine;


public interface SQLQueryResultListener<T> {

	public void onResult(T result);

}
