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
	
	private final static int MAX_SIZE = 500;	
	
	private LinkedHashMap<String, UserStat> userStatMap = new LinkedHashMap<String, UserStat>();	
	private ReentrantReadWriteLock  lock  = new ReentrantReadWriteLock();
	
    private final static UserStatFilter instance  = new UserStatFilter();
    
    public static UserStatFilter getInstance() {
        return instance;
    }  
	
	private UserStatFilter() {		
		
		this.userStatMap = new LinkedHashMap<String, UserStat>(16, 0.75f, false) {			
			private static final long serialVersionUID = 1L;			
			protected boolean removeEldestEntry(Map.Entry<String, UserStat> eldest) {
				boolean remove = (size() > MAX_SIZE);
				return remove;
			}
		};
	}
	
	public void updateStat(String user, int sqlType, String sql, long startTime) {	
		
		UserStat userStat = createUserStat(user);
		userStat.update(sqlType, sql, startTime);		
	}
	
	private UserStat createUserStat(String user) {
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
	
	public UserStat getUserStatByUser(String user) {		
		lock.readLock().lock();
        try {
            for (Map.Entry<String, UserStat> entry : this.userStatMap.entrySet()) {
                if ( entry.getValue().getUser().equals(user) ) {
                    return entry.getValue();
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
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
