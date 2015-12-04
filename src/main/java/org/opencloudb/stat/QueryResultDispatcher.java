package org.opencloudb.stat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * SQL执行后的派发  QueryResult 事件
 * 
 * @author zhuam
 *
 */
public class QueryResultDispatcher {
	
	private static List<QueryResultListener> listeners = new CopyOnWriteArrayList<QueryResultListener>();
	
	// 初始化强制加载
	static {
		listeners.add( UserStatAnalyzer.getInstance() );
		listeners.add( TableStatAnalyzer.getInstance() );
	}
	
	public static void addListener(QueryResultListener listener) {
		if (listener == null) {
			throw new NullPointerException();
		}
		listeners.add(listener);
	}

	public static void removeListener(QueryResultListener listener) {
		listeners.remove(listener);
	}

	public static void removeAllListener() {
		listeners.clear();
	}
	

	public static void dispatchQuery(QueryResult query) {
		
		//注入 结束时间
		long now = System.currentTimeMillis();
		query.setEndTime( now );
		
		for(QueryResultListener listener: listeners) {
			listener.onQuery( query );
		}
	}

}
