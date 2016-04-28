package org.opencloudb.stat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opencloudb.MycatServer;
import org.opencloudb.server.parser.ServerParse;

/**
 * 按访问用户 计算SQL的运行状态
 * 
 * @author Ben
 *
 */
public class UserStatAnalyzer implements QueryResultListener {
	
	private LinkedHashMap<String, UserStat> userStatMap = new LinkedHashMap<String, UserStat>();	
	private ReentrantReadWriteLock  lock  = new ReentrantReadWriteLock();
	
    private final static UserStatAnalyzer instance  = new UserStatAnalyzer();
    
    private UserStatAnalyzer() {
    }
    
    public static UserStatAnalyzer getInstance() {
        return instance;
    }  
	
	public void setSlowTime(long time) {
		//this.SQL_SLOW_TIME = time;
		MycatServer.getInstance().getConfig().getSystem().setSlowTime(time);
	}
	
	@Override
	public void onQueryResult(QueryResult query) {
		
		int sqlType = query.getSqlType();
		String sql = query.getSql();
		
		switch(sqlType) {
    	case ServerParse.SELECT:		
    	case ServerParse.UPDATE:			
    	case ServerParse.INSERT:		
    	case ServerParse.DELETE:
    	case ServerParse.REPLACE:  	
    		
    		String user = query.getUser();
    		long startTime = query.getStartTime();
    		long endTime = query.getEndTime();
    		String ip=query.getIp();
    		
    		this.lock.writeLock().lock();
            try {
            	UserStat userStat = userStatMap.get(user);
                if (userStat == null) {
                    userStat = new UserStat(user);
                    userStatMap.put(user, userStat);
                }                
                userStat.update(sqlType, sql, ip,startTime, endTime);	
                
            } finally {
            	this.lock.writeLock().unlock();
            }	
		}
	}	

	
	public Map<String, UserStat> getUserStatMap() {
		Map<String, UserStat> map = new LinkedHashMap<String, UserStat>(userStatMap.size());
		this.lock.readLock().lock();
        try {
            map.putAll(userStatMap);
        } finally {
        	this.lock.readLock().unlock();
        }
        return map;
	}
}
