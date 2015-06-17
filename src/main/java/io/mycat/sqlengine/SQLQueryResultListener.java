package io.mycat.sqlengine;


public interface SQLQueryResultListener<T> {

	public void onRestult(T result);

}
