package org.opencloudb.stat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
	
	@Override
	public void onQuery(QueryResult query) {
		
		String user = query.getUser();
		int sqlType = query.getSqlType();
		String sql = query.getSql();
		long startTime = query.getStartTime();
		long endTime = query.getEndTime();
		
		UserStat userStat = getUserStat(user);
		userStat.update(sqlType, sql, startTime, endTime);		
	}	

	private UserStat getUserStat(String user) {
        lock.writeLock().lock();
        try {
        	UserStat userStat = userStatMap.get(user);
            if (userStat == null) {
                userStat = new UserStat(user);
                userStatMap.put(user, userStat);
            }
            return userStat;
        } finally {
            lock.writeLock().unlock();
        }
    }	
	
	public Map<String, UserStat> getUserStatMap() {
		Map<String, UserStat> map = new LinkedHashMap<String, UserStat>(userStatMap.size());
        lock.readLock().lock();
        try {
            map.putAll(userStatMap);
        } finally {
            lock.readLock().unlock();
        }
        return map;
	}
}
