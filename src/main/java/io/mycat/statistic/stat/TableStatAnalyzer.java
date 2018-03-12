package io.mycat.statistic.stat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.druid.util.JdbcConstants;

import io.mycat.server.parser.ServerParse;
import io.mycat.util.StringUtil;

/**
 * 按SQL表名进行计算
 * 
 * @author zhuam
 *
 */
public class TableStatAnalyzer implements QueryResultListener {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TableStatAnalyzer.class);
	
	private Map<String, TableStat> tableStatMap = new ConcurrentHashMap<>();
	private ReentrantLock lock  = new ReentrantLock();
	
	//解析SQL 提取表名
	private SQLParser sqlParser = new SQLParser();
	
    private final static TableStatAnalyzer instance  = new TableStatAnalyzer();
    
    private TableStatAnalyzer() {}
    
    public static TableStatAnalyzer getInstance() {
        return instance;
    }  
    
	@Override
	public void onQueryResult(QueryResult queryResult) {
		
		int sqlType = queryResult.getSqlType();
		String sql = queryResult.getSql();
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
    			tableStat.update(sqlType, sql, queryResult.getStartTime(), queryResult.getEndTime(), relaTables);		
    		}    		
    		break;
    	}		
	}	
	
	private TableStat getTableStat(String tableName) {
		TableStat userStat = tableStatMap.get(tableName);
		if (userStat == null) {
			if(lock.tryLock()){
				try{
					userStat = new TableStat(tableName);
					tableStatMap.put(tableName, userStat);
				} finally {
					lock.unlock();
				}
			}else{
				while(userStat == null){
					userStat = tableStatMap.get(tableName);
				}
			}
		}
		return userStat;
    }
	
	public Map<String, TableStat> getTableStatMap() {
		Map<String, TableStat> map = new LinkedHashMap<String, TableStat>(tableStatMap.size());
		map.putAll(tableStatMap);
        return map;
	}
	
	/**
	 * 获取 table 访问排序统计
	 */
	public List<TableStat> getTableStats(boolean isClear) {
		SortedSet<TableStat> tableStatSortedSet = new TreeSet<>(tableStatMap.values());
		List<TableStat> list =  new ArrayList<>(tableStatSortedSet);
        return list;
	}	
	
	public void ClearTable() {
		tableStatMap.clear();
	}

	
	/**
	 * 解析 table name
	 */
	private static class SQLParser {
		
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
		  try{			
			
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
				
				//TODO: modify by owenludong
				String dbType = ((SQLSelectStatement) stmt).getDbType();
				if( !StringUtil.isEmpty(dbType) && JdbcConstants.MYSQL.equals(dbType) ){
					stmt.accept(new MySqlASTVisitorAdapter() {
						public boolean visit(SQLExprTableSource x){
							tables.add( fixName( x.toString() ) );
							return super.visit(x);
						}
					});
					
				} else {
					stmt.accept(new SQLASTVisitorAdapter() {
						public boolean visit(SQLExprTableSource x){
							tables.add( fixName( x.toString() ) );
							return super.visit(x);
						}
					});
				}
			}	
		  } catch (Exception e) {
			  LOGGER.error("TableStatAnalyzer err:",e.toString());
		  }
		  
		 return tables;
		}
	}	
	
	
/*	public static void main(String[] args) {
		
		List<String> sqls = new ArrayList<String>();
		
		sqls.add( "SELECT id, name, age FROM v1select1 a LEFT OUTER JOIN v1select2 b ON  a.id = b.id WHERE a.name = 12 ");
		sqls.add( "insert into v1user_insert(id, name) values(1,3)");
		sqls.add( "delete from v1user_delete where id= 2");
		sqls.add( "update v1user_update set id=2 where id=3");
		sqls.add( "select ename,deptno,sal from v1user_subquery1 where deptno=(select deptno from v1user_subquery2 where loc='NEW YORK')");
		sqls.add( "replace into v1user_insert(id, name) values(1,3)");
		sqls.add( "select * from v1xx where id=3 group by zz");
		sqls.add( "select * from v1yy where xx=3 limit 0,3");
		sqls.add( "SELECT * FROM (SELECT * FROM posts ORDER BY dateline DESC) GROUP BY  tid ORDER BY dateline DESC LIMIT 10");
		
		for(String sql: sqls) {
			List<String> tables = TableStatAnalyzer.getInstance().sqlParser.parseTableNames(sql);
			for(String t: tables) {
				System.out.println( t );
			}
		}		
	}
	*/

}
