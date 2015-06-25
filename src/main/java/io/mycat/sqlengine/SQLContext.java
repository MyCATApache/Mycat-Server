package io.mycat.sqlengine;

import io.mycat.backend.BackendConnection;
import io.mycat.route.RouteResultsetNode;

import java.util.concurrent.ConcurrentHashMap;

/**
 * sql context used for execute sql
 * 
 * @author wuzhih
 * 
 */
public class SQLContext {
	private ConcurrentHashMap<RouteResultsetNode, BackendConnection> target;
	private String curentSQL;
	private boolean autoCommit;

}
