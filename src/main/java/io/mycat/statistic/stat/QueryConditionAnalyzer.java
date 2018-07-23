package io.mycat.statistic.stat;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Condition;
import io.mycat.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 特定 SQL 查询条件的统计分析
 * --------------------------------------------------
 * 
 * 例:
 * SELECT * FROM v1user Where userName=? AND cityName =?
 * SELECT * FROM v1user Where userName=?
 * SELECT * FROM v1user Where userName=? AND age > 20
 * 
 * SELECT * FROM v1user Where userName = "张三" AND cityName = "北京";
 * SELECT * FROM v1user Where userName = "李四" 
 * SELECT * FROM v1user Where userName = "张三" AND age > 20
 * 
 * 现在我们希望知道DB 中 业务比较关注的 userName 有哪些，次数是多少, 怎么处理哩，如下
 * 
 * 设置： 表名&条件列  ( v1user&userName ) 即可，取消请设置 NULL
 * 
 * @author zhuam
 *
 */
public class QueryConditionAnalyzer implements QueryResultListener {
	private final static long MAX_QUERY_MAP_SIZE = 100000;
	private static final Logger LOGGER = LoggerFactory.getLogger(QueryConditionAnalyzer.class);
	
	private String tableName = null;
	private String columnName = null;
	
	// column value -> count
//	private final HashMap<Object, Long> map = new HashMap<Object, Long>();
	private final Map<Object, AtomicLong> map = new ConcurrentHashMap<>();

	private ReentrantLock lock = new ReentrantLock();
	
	private SQLParser sqlParser = new SQLParser();
    
    private final static QueryConditionAnalyzer instance  = new QueryConditionAnalyzer();
    
    private QueryConditionAnalyzer() {}
    
    public static QueryConditionAnalyzer getInstance() {
        return instance;
    }  
    
	
	@Override
	public void onQueryResult(QueryResult queryResult) {
		
//		this.lock.lock();
//		try {
			
			int sqlType = queryResult.getSqlType();
			String sql = queryResult.getSql();
	
			switch(sqlType) {
	    	case ServerParse.SELECT:		
    			List<Object> values = sqlParser.parseConditionValues(sql, this.tableName, this.columnName);
	    		if ( values != null ) {
	    			
	    			if ( this.map.size() < MAX_QUERY_MAP_SIZE ) {
	    				
		    			for(Object value : values) {
							AtomicLong count = this.map.get(value);
		    				if (count == null) {
		    					count = new AtomicLong(1L);
		    				} else {
		    					count.getAndIncrement();
		    				}	    				
		    				this.map.put(value, count);	    				
		    			}
		    			
	    			} else {
	    				LOGGER.debug(" this map is too large size ");
	    			}
	    		}
			}	
			
//		} finally {
//			this.lock.unlock();
//		}
	}
	
	public boolean setCf(String cf) {
		
		boolean isOk = false;
		
		this.lock.lock();  
		try {  
			
			if ( !"NULL".equalsIgnoreCase(cf) ) {
				
				String[] table_column = cf.split("&");
				if ( table_column != null && table_column.length == 2 ) {					
					this.tableName = table_column[0];
					this.columnName = table_column[1];
					this.map.clear();
					
					isOk = true;
				}	
				
			} else {	
				
				this.tableName = null;
				this.columnName = null;				
				this.map.clear();				
				
				isOk = true;
			}
			
		} finally {  
			this.lock.unlock();   
		}  
		
		return isOk;		
	}
	
	public String getKey() {
		return this.tableName + "." + this.columnName;
	}
	
	public List<Map.Entry<Object, AtomicLong>> getValues() {
		List<Map.Entry<Object, AtomicLong>> list = new ArrayList<Map.Entry<Object, AtomicLong>>(map.entrySet());
		return list;
	}
	
	
    // SQL 解析
	class SQLParser {
		
		/**
		 * 去掉库名、去掉``
		 * @param tableName
		 * @return
		 */
		private String fixName(String tableName) {
			if ( tableName != null ) {
				tableName = tableName.replace("`", "");
				int dotIdx = tableName.indexOf(".");
				if ( dotIdx > 0 ) {
					tableName = tableName.substring(1 + dotIdx).trim();
				}
			}
			return tableName;
		}
		
		/**
		 * 解析 SQL 获取指定表及条件列的值
		 * 
		 * @param sql
		 * @param tableName
		 * @param colnumName
		 * @return
		 */
		public List<Object> parseConditionValues(String sql, String tableName, String colnumName)  {
			
			List<Object> values = null;
			
			if ( sql != null && tableName != null && columnName != null ) {
			
				values = new ArrayList<Object>();
				
				MySqlStatementParser parser = new MySqlStatementParser(sql);
				SQLStatement stmt = parser.parseStatement();
				
				MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
				stmt.accept(visitor);
				
				String currentTable = visitor.getCurrentTable();
				if ( tableName.equalsIgnoreCase( currentTable ) ) {
					
					List<Condition> conditions = visitor.getConditions();
					for(Condition condition: conditions) {
						
						String ccN = condition.getColumn().getName();
						ccN = fixName(ccN);
						
						if ( colnumName.equalsIgnoreCase( ccN ) ) {					
							List<Object> ccVL = condition.getValues();
							values.addAll( ccVL );
						}
					}
				}				
			}
			return values;
		}		
	}
	
   /* -----------------------------------------------------------------
    public static void main(String arg[]) {
    	
    	String sql = "SELECT `fnum`, `forg`, `fdst`, `airline`, `ftype` , `ports_of_call`, " +
					"`scheduled_deptime`, `scheduled_arrtime`, `actual_deptime`, `actual_arrtime`, " +
					"`flight_status_code` FROM dynamic " +
					"WHERE `fnum` = 'CA123'  AND `forg` = 'PEK'  AND `fdst` = 'SHA' " +
					"AND `scheduled_deptime` BETWEEN 1212121 AND 232323233 " +
					"AND `fservice` = 'J' AND `fcategory` = 1 " +
					"AND `share_execute_flag` = 1 ORDER BY scheduled_deptime";
    	
    	QueryResult qr = new QueryResult("zhuam", ServerParse.SELECT, sql, 0);
    	
    	QueryConditionAnalyzer analyzer = QueryConditionAnalyzer.getInstance();
    	analyzer.setTableColumnFilter("dynamic&fnum");
    	analyzer.onQuery(qr);
    	
    	List<Map.Entry<Object, Long>> list = analyzer.getValues();
    	System.out.println( list );
     }
    */
}