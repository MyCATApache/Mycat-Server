package io.mycat.statistic.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

public class UserSqlHighStat {
	
	private static final int CAPACITY_SIZE = 100;
	private static final int DELETE_SIZE = 10;
	
	private LinkedHashMap<String, SqlFrequency> sqlFrequencyMap = new LinkedHashMap<String, SqlFrequency>();	
	
	
	private SqlParser sqlParser = new SqlParser();
	
	public void addSql(String sql, long executeTime,long startTime, long endTime ){
    	if ( this.sqlFrequencyMap.size() >= CAPACITY_SIZE ) {
    		
    		// 删除频率次数排名靠后的SQL
    		List<Map.Entry<String, SqlFrequency>> list = this.sortFrequency( sqlFrequencyMap, true );
    		for (int i = 0; i < DELETE_SIZE; i++) {
    			
    			Entry<String, SqlFrequency> entry = list.get(i);
    			String key = entry.getKey();
    			this.sqlFrequencyMap.remove( key );
    		}
    	}
    	String newSql = this.sqlParser.mergeSql(sql);
    	SqlFrequency frequency = this.sqlFrequencyMap.get( newSql );
        if ( frequency == null) {
        	frequency = new SqlFrequency();
        	frequency.setSql( newSql );
        } 
        frequency.setLastTime( endTime );
        frequency.incCount();
        frequency.setExecuteTime(executeTime);
        this.sqlFrequencyMap.put(newSql, frequency);        
	}		

	
	/**
	 * 获取 SQL 访问频率
	 */
	public List<Map.Entry<String, SqlFrequency>> getSqlFrequency(boolean isClear) {
		
		List<Map.Entry<String, SqlFrequency>> list = this.sortFrequency( sqlFrequencyMap, false );
        if ( isClear ) {
        	clearSqlFrequency();  // 获取 高频SQL后清理
        }
        
        return list;
	}	
	
	
	private void clearSqlFrequency() {		
		sqlFrequencyMap.clear();
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
					return (int)(o2.getValue().getCount() - o1.getValue().getCount()); // 降序
				} else {
					return (int)(o1.getValue().getCount() - o2.getValue().getCount()); // 升序
				}
			}
		});

		return list;
	}
	
	class SqlParser {
		
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
