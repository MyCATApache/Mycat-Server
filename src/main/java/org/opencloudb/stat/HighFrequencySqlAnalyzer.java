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

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

/**
 * 高频SQL 
 * 
 * @author zhuam
 *
 */
public class HighFrequencySqlAnalyzer implements QueryResultListener {
	
	private static final int CAPACITY_SIZE = 10;
	private static final int DELETE_SIZE = 2;
	
	private LinkedHashMap<String, SqlFrequency> sqlFrequencyMap = new LinkedHashMap<String, SqlFrequency>();	
	private ReentrantReadWriteLock lock  = new ReentrantReadWriteLock();
	
	private SQLParser sqlParser = new SQLParser();
	
    private final static HighFrequencySqlAnalyzer instance  = new HighFrequencySqlAnalyzer();
    
    private HighFrequencySqlAnalyzer() {}
    
    public static HighFrequencySqlAnalyzer getInstance() {
        return instance;
    }  

	@Override
	public void onQuery(QueryResult query) {
		
		String sql = query.getSql();		
		String newSql = this.sqlParser.mergeSql(sql);
		
		this.lock.writeLock().lock();
        try {
        	
        	if ( this.sqlFrequencyMap.size() >= CAPACITY_SIZE ) {
        		
        		// 删除频率次数排名靠后的SQL
        		List<Map.Entry<String, SqlFrequency>> list = this.sortFrequency( sqlFrequencyMap, false );
        		for (int i = 0; i < DELETE_SIZE; i++) {
        			
        			Entry<String, SqlFrequency> entry = list.get(i);
        			String key = entry.getKey();
        			this.sqlFrequencyMap.remove( key );
        		}
        	}
        	
        	SqlFrequency frequency = this.sqlFrequencyMap.get( newSql );
            if ( frequency == null) {
            	frequency = new SqlFrequency();
            	frequency.setSql( newSql );
            } 
            frequency.setLastTime( query.getEndTime() );
            frequency.incCount();
            this.sqlFrequencyMap.put(newSql, frequency);
            
        } finally {
        	this.lock.writeLock().unlock();
        }
	}

	/**
	 * 获取 SQL 访问频率
	 */
	public Map<String, SqlFrequency> getSqlFrequency() {
		Map<String, SqlFrequency> map = new LinkedHashMap<String, SqlFrequency>( sqlFrequencyMap.size() );
        lock.readLock().lock();
        try {
            map.putAll( sqlFrequencyMap );
        } finally {
            lock.readLock().unlock();
        }
        return map;
	}	
	
	/**
	 * 排序
	 */
	private List<Map.Entry<String, SqlFrequency>> sortFrequency(HashMap<String, SqlFrequency> map,
			final boolean bAsc) {

		List<Map.Entry<String, SqlFrequency>> list = new ArrayList<Map.Entry<String, SqlFrequency>>(map.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, SqlFrequency>>() {
			public int compare(Map.Entry<String, SqlFrequency> o1, Map.Entry<String, SqlFrequency> o2) {

				if (!bAsc) {
					return o2.getValue().getCount() - o1.getValue().getCount(); // 降序
				} else {
					return o1.getValue().getCount() - o2.getValue().getCount(); // 升序
				}
			}
		});

		return list;

	}
	
	public class SqlFrequency {
		
		private String sql;
		private int count = 0;
		private long lastTime = 0;

		public String getSql() {
			return sql;
		}

		public void setSql(String sql) {
			this.sql = sql;
		}

		public int getCount() {
			return count;
		}

		public void incCount() {
			this.count++;
		}

		public long getLastTime() {
			return lastTime;
		}

		public void setLastTime(long lastTime) {
			this.lastTime = lastTime;
		}			
	}
	
	
	class SQLParser {
		
		public String fixSql(String sql) {
			if ( sql != null)
				return sql.replace("\n", " ");
			return sql;
	    }
		
		public String mergeSql(String sql) {
			
			String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, "mysql");
			return fixSql( newSql );
	    }
	}

}
