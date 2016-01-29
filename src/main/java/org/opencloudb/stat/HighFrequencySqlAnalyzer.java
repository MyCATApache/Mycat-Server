package org.opencloudb.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opencloudb.server.parser.ServerParse;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

/**
 * 高频SQL 
 * 
 * @author zhuam
 *
 */
public class HighFrequencySqlAnalyzer implements QueryResultListener {
	
	private ReentrantReadWriteLock lock  = new ReentrantReadWriteLock();
	
    private final static HighFrequencySqlAnalyzer instance  = new HighFrequencySqlAnalyzer();
    
    private HighFrequencySqlAnalyzer() {}
    
    public static HighFrequencySqlAnalyzer getInstance() {
        return instance;
    }  

	@Override
	public void onQueryResult(QueryResult queryResult) {
		
		int sqlType = queryResult.getSqlType();
		String sql = queryResult.getSql();		
	//	String newSql = this.sqlParser.mergeSql(sql);
		long executeTime = queryResult.getEndTime() - queryResult.getStartTime();
		this.lock.writeLock().lock();
        try {
        	
        	switch(sqlType) {
        	case ServerParse.SELECT:		
        	case ServerParse.UPDATE:			
        	case ServerParse.INSERT:		
        	case ServerParse.DELETE:
        	case ServerParse.REPLACE:          		
        	}
            
        } finally {
        	this.lock.writeLock().unlock();
        }


	}
	
}