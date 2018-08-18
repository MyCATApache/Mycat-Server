package io.mycat.statistic.stat;

import io.mycat.server.parser.ServerParse;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 按访问用户 计算SQL的运行状态
 * 
 * @author Ben
 *
 */
public class UserStatAnalyzer implements QueryResultListener {
	
	private LinkedHashMap<String, UserStat> userStatMap = new LinkedHashMap<String, UserStat>();	
	
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
    		String user = query.getUser();
    		int sqlType = query.getSqlType();
    		String sql = query.getSql();
    		long sqlRows = query.getSqlRows();
    		long netInBytes = query.getNetInBytes();
    		long netOutBytes = query.getNetOutBytes();
    		long startTime = query.getStartTime();
    		long endTime = query.getEndTime();
    		int resultSetSize=query.getResultSize();
        	UserStat userStat = userStatMap.get(user);
            if (userStat == null) {
                userStat = new UserStat(user);
                userStatMap.put(user, userStat);
            }                
            userStat.update(sqlType, sql, sqlRows, netInBytes, netOutBytes, startTime, endTime,resultSetSize);	
            break;
		}
	}
	
	public Map<String, UserStat> getUserStatMap() {		
		Map<String, UserStat> map = new LinkedHashMap<String, UserStat>(userStatMap.size());	
		map.putAll(userStatMap);
        return map;
	}
}
