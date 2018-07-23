package io.mycat.statistic.stat;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 大结果集 SQL
 *
 */
public class SqlResultSizeRecorder {
	
	private ConcurrentHashMap<String, SqlResultSet> sqlResultSetMap = new ConcurrentHashMap<String, SqlResultSet>();	
	

	
	public void addSql(String sql,int resultSetSize ){
		SqlResultSet sqlResultSet;
		SqlParser sqlParserHigh = new SqlParser();
		sql=sqlParserHigh.mergeSql(sql);
		if(this.sqlResultSetMap.containsKey(sql)){
		    sqlResultSet =this.sqlResultSetMap.get(sql);
			sqlResultSet.count();
			sqlResultSet.setSql(sql);
			System.out.println(sql);
			sqlResultSet.setResultSetSize(resultSetSize);
		}else{
		sqlResultSet = new SqlResultSet();
		sqlResultSet.setResultSetSize(resultSetSize);
		sqlResultSet.setSql(sql);
        this.sqlResultSetMap.put(sql, sqlResultSet);     
		}
	}		

	
	/**
	 * 获取 SQL 大结果集记录
	 */
	public ConcurrentHashMap<String, SqlResultSet>  getSqlResultSet() {
		
        return sqlResultSetMap;
	}	
	
	
	public void clearSqlResultSet() {		
		sqlResultSetMap.clear();
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