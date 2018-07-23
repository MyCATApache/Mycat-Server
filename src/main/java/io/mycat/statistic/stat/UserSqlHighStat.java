package io.mycat.statistic.stat;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class UserSqlHighStat {
	
	private static final int CAPACITY_SIZE = 1024;

	private Map<String,SqlFrequency> sqlFrequencyMap = new ConcurrentHashMap<>();

	private ReentrantLock lock = new ReentrantLock();

	
	private SqlParser sqlParser = new SqlParser();
	
	public void addSql(String sql, long executeTime,long startTime, long endTime ){
    	String newSql = this.sqlParser.mergeSql(sql);
    	SqlFrequency frequency = this.sqlFrequencyMap.get(newSql);
        if ( frequency == null) {
			//防止新建的时候的并发问题，只有新建的时候有锁
			if(lock.tryLock()){
        		try{
					frequency = new SqlFrequency();
        			frequency.setSql( newSql );
				} finally {
					lock.unlock();
				}
			} else{
				while(frequency == null){
					frequency = this.sqlFrequencyMap.get(newSql);
				}
			}
        } 
        frequency.setLastTime( endTime );
        frequency.incCount();
		//TODO 目前setExecuteTime方法由于弃用锁，所以某些参数不准确，为了性能，放弃这些参数的准确性。下一步期待更多优化
        frequency.setExecuteTime(executeTime);
        this.sqlFrequencyMap.put(newSql, frequency);        
	}

	
	/**
	 * 获取 SQL 访问频率
	 */
	public List<SqlFrequency> getSqlFrequency(boolean isClear) {
		List<SqlFrequency> list = new ArrayList<>(this.sqlFrequencyMap.values());
		if(isClear){
			clearSqlFrequency();
		}
        return list;
	}	
	
	
	private void clearSqlFrequency() {		
		sqlFrequencyMap.clear();
	}

	public void recycle() {
		if(sqlFrequencyMap.size() > CAPACITY_SIZE){
			Map<String,SqlFrequency> sqlFrequencyMap2 = new ConcurrentHashMap<>();
			SortedSet<SqlFrequency> sqlFrequencySortedSet = new TreeSet<>(this.sqlFrequencyMap.values());
			List<SqlFrequency> keyList = new ArrayList<SqlFrequency>(sqlFrequencySortedSet);
			int i = 0;
			for(SqlFrequency key : keyList){
				if(i == CAPACITY_SIZE) {
					break;
				}
				sqlFrequencyMap2.put(key.getSql(),key);
				i++;
			}
			sqlFrequencyMap = sqlFrequencyMap2;
		}
	}
	

	
	private static class SqlParser {
		
		public String fixSql(String sql) {
			if ( sql != null) {
				return sql.replace("\n", " ");
			}
			return sql;
	    }
		
		public String mergeSql(String sql) {
			
			String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, "mysql");
			return fixSql( newSql );
	    }

	}
	
}
