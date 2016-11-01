package org.opencloudb.stat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;

/**
 * SQL执行后的派发  QueryResult 事件
 * 
 * @author zhuam
 *
 */
public class QueryResultDispatcher {
	
	// 是否派发 QueryResult 事件
	private final static AtomicBoolean isClosed = new AtomicBoolean(false);
	
	private static final Logger LOGGER = Logger.getLogger(QueryResultDispatcher.class);
	
	private static List<QueryResultListener> listeners = new CopyOnWriteArrayList<QueryResultListener>();

	// 初始化强制加载
	static {
		listeners.add( UserStatAnalyzer.getInstance() );
		listeners.add( TableStatAnalyzer.getInstance() );
		//listeners.add( HighFrequencySqlAnalyzer.getInstance() );
		listeners.add( QueryConditionAnalyzer.getInstance() );
	}
	
	public static boolean close() {
		if (isClosed.compareAndSet(false, true)) {
			return true;
		}
		return false;
	}
	
	public static boolean open() {
		if (isClosed.compareAndSet(true, false)) {
			return true;
		}
		return false;
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
	
	public static void dispatchQuery(final QueryResult queryResult) {
		
		if ( isClosed.get() ) {
			return;
		}
		
		//TODO：异步分发，待进一步调优 
		MycatServer.getInstance().getBusinessExecutor().execute(new Runnable() {
			
			public void run() {		
				
				for(QueryResultListener listener: listeners) {
					try {
						listener.onQueryResult( queryResult );
					} catch(Exception e) {
						LOGGER.error("error",e);
					}
				}					
			}
		});
	}

}
