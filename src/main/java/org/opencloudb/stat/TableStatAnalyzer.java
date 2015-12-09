package org.opencloudb.stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.opencloudb.server.parser.ServerParse;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;

/**
 * 按SQL表名进行计算
 * 
 * @author zhuam
 *
 */
public class TableStatAnalyzer implements QueryResultListener {
	
	private LinkedHashMap<String, TableStat> tableStatMap = new LinkedHashMap<String, TableStat>();	
	private ReentrantReadWriteLock  lock  = new ReentrantReadWriteLock();
	
	//解析SQL 提取表名
	private SQLParser sqlParser = new SQLParser();
	
    private final static TableStatAnalyzer instance  = new TableStatAnalyzer();
    
    private TableStatAnalyzer() {}
    
    public static TableStatAnalyzer getInstance() {
        return instance;
    }  
    
	@Override
	public void onQuery(QueryResult query) {
		
		int sqlType = query.getSqlType();
		String sql = query.getSql();

		switch(sqlType) {
    	case ServerParse.SELECT:		
    	case ServerParse.UPDATE:			
    	case ServerParse.INSERT:		
    	case ServerParse.DELETE:
    	case ServerParse.REPLACE:  
    		
    		//关联表提取
    		String masterTable = null;
    		List<String> relaTables = new ArrayList<String>();
    		
    		List<String> tables = sqlParser.parseTableNames(sql);
    		for(int i = 0; i < tables.size(); i++) {
    			String table = tables.get(i);
    			if ( i == 0 ) {
    				masterTable = table;
    			} else {
    				relaTables.add( table );
    			}
    		}
    		
    		if ( masterTable != null ) {
    			TableStat tableStat = getTableStat( masterTable );
    			tableStat.update(sqlType, sql, query.getStartTime(), query.getEndTime(), relaTables);		
    		}    		
    		break;
    	}		
	}	
	
	private TableStat getTableStat(String tableName) {
        lock.writeLock().lock();
        try {
        	TableStat userStat = tableStatMap.get(tableName);
            if (userStat == null) {
                userStat = new TableStat(tableName);
                tableStatMap.put(tableName, userStat);
            }
            return userStat;
        } finally {
            lock.writeLock().unlock();
        }
    }	
	
	public Map<String, TableStat> getTableStatMap() {
		Map<String, TableStat> map = new LinkedHashMap<String, TableStat>(tableStatMap.size());
        lock.readLock().lock();
        try {
            map.putAll(tableStatMap);
        } finally {
            lock.readLock().unlock();
        }
        return map;
	}
	
	/**
	 * 获取 table 访问排序统计
	 */
	public List<Map.Entry<String, TableStat>> getTableStats() {
		
		List<Map.Entry<String, TableStat>> list = null;
		
        lock.readLock().lock();
        try {
        	list = this.sortTableStats(tableStatMap , false );
        } finally {
            lock.readLock().unlock();
        }
        return list;
	}	
	/**
	 * 排序
	 */
	private List<Map.Entry<String, TableStat>> sortTableStats(HashMap<String, TableStat> map,
			final boolean bAsc) {

		List<Map.Entry<String, TableStat>> list = new ArrayList<Map.Entry<String, TableStat>>(map.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, TableStat>>() {
			public int compare(Map.Entry<String, TableStat> o1, Map.Entry<String, TableStat> o2) {

				if (!bAsc) {
					return o2.getValue().getCount() - o1.getValue().getCount(); // 降序
				} else {
					return o1.getValue().getCount() - o2.getValue().getCount(); // 升序
				}
			}
		});

		return list;

	}	
	/**
	 * 解析 table name
	 */
	class SQLParser {
		
		private SQLStatement parseStmt(String sql) {
			SQLStatementParser statParser = SQLParserUtils.createSQLStatementParser(sql, "mysql");
			SQLStatement stmt = statParser.parseStatement();
			return stmt;		
		}		
		
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
		 * 解析 SQL table name
		 */
		public List<String> parseTableNames(String sql) {
			
			final List<String> tables = new ArrayList<String>();
			
			SQLStatement stmt = parseStmt(sql);
			if (stmt instanceof MySqlReplaceStatement ) {
				String table = ((MySqlReplaceStatement)stmt).getTableName().getSimpleName();
				tables.add( fixName( table ) );
				
			} else if (stmt instanceof SQLInsertStatement ) {
				String table = ((SQLInsertStatement)stmt).getTableName().getSimpleName();
				tables.add( fixName( table ) );
				
			} else if (stmt instanceof SQLUpdateStatement ) {
				String table = ((SQLUpdateStatement)stmt).getTableName().getSimpleName();
				tables.add( fixName( table ) );
				
			} else if (stmt instanceof SQLDeleteStatement ) {
				String table = ((SQLDeleteStatement)stmt).getTableName().getSimpleName();
				tables.add( fixName( table ) );
				
			} else if (stmt instanceof SQLSelectStatement ) {
				stmt.accept(new SQLASTVisitorAdapter() {	
					public boolean visit(SQLExprTableSource x){
						tables.add( fixName( x.toString() ) );
						return super.visit(x);
					}
				});
			}			
			return tables;
		}
	}	

}