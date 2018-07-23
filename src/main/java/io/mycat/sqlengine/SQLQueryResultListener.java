package io.mycat.sqlengine;

/**
 * SQL查询结果监听
 * @param <T>
 */
public interface SQLQueryResultListener<T> {

	public void onResult(T result);

}
