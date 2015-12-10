package org.opencloudb.stat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;

/**
 * SQL执行后的派发  QueryResult 事件
 * 
 * @author zhuam
 *
 */
public class QueryResultDispatcher {
	
	private static final Logger LOGGER = Logger.getLogger(QueryResultDispatcher.class);
	
	private static List<QueryResultListener> listeners = new CopyOnWriteArrayList<QueryResultListener>();

	// 初始化强制加载
	static {
		listeners.add( UserStatAnalyzer.getInstance() );
		listeners.add( TableStatAnalyzer.getInstance() );
		listeners.add( HighFrequencySqlAnalyzer.getInstance() );
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
	
	public static void dispatchQuery(final QueryResult query) {
		
		//TODO：异步分发，待进一步调优 
		MycatServer.getInstance().getBusinessExecutor().execute(new Runnable() {
			
			public void run() {				
				//注入 结束时间
				long now = System.currentTimeMillis();
				query.setEndTime( now );
				
				for(QueryResultListener listener: listeners) {
					try {
						listener.onQuery( query );
					} catch(Exception e) {
						LOGGER.error(e);
					}
				}
			}
		});
	}

}