package io.mycat.statistic.stat;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.mycat.server.parser.ServerParse;

/**
 * 按访问用户 计算SQL的运行状态
 * 
 * @author Ben
 *
 */
public class UserStatAnalyzer implements QueryResultListener {
	
	private Cache<String, UserStat> userStatMap = CacheBuilder.newBuilder().maximumSize(8192).build();
	
    private final static UserStatAnalyzer instance  = new UserStatAnalyzer();
    
    private UserStatAnalyzer() {
    }
    
    public static UserStatAnalyzer getInstance() {
        return instance;
    }  
	
	@Override
	public void onQueryResult(QueryResult query) {		
		switch( query.getSqlType() ) {
    	case ServerParse.SELECT:		
    	case ServerParse.UPDATE:			
    	case ServerParse.INSERT:		
    	case ServerParse.DELETE:
    	case ServerParse.REPLACE:
			String host = query.getHost();
			if (host==null){
				host = "";
			}
    		String user = query.getUser();
    		int sqlType = query.getSqlType();
    		String sql = query.getSql();
    		long sqlRows = query.getSqlRows();
    		long netInBytes = query.getNetInBytes();
    		long netOutBytes = query.getNetOutBytes();
    		long startTime = query.getStartTime();
    		long endTime = query.getEndTime();
    		int resultSetSize=query.getResultSize();
        	UserStat userStat = userStatMap.getIfPresent(user);
            if (userStat == null) {
                userStat = new UserStat(user);
                userStatMap.put(user, userStat);
            }                
            userStat.update(sqlType, sql, sqlRows, netInBytes, netOutBytes, startTime, endTime,resultSetSize,host);
            break;
		}
	}
	
	public Map<String, UserStat> getUserStatMap() {
		return new HashMap<>(userStatMap.asMap());
	}
}
