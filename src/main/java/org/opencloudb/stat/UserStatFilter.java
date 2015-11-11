package org.opencloudb.stat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 管理用户状态
 * 
 * @author Ben
 *
 */
public class UserStatFilter {
	
	private LinkedHashMap<String, UserStat> userStatMap = new LinkedHashMap<String, UserStat>();	
	private ReentrantReadWriteLock  lock  = new ReentrantReadWriteLock();
	
    private final static UserStatFilter instance  = new UserStatFilter();
    
    public static UserStatFilter getInstance() {
        return instance;
    }  
	
	/**
	 * update status
	 */
	public void updateStat(String user, int sqlType, String sql, long startTime) {	
		
		UserStat userStat = getUserStat(user);
		userStat.update(sqlType, sql, startTime);		
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
