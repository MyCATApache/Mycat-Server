package io.mycat.statistic.stat;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class UserSqlLargeStat {
	
    private final int count;
    private SortedSet<SqlLarge> sqls;

    public UserSqlLargeStat(int count) {
        this.count = count;
        this.sqls = new ConcurrentSkipListSet<>();
    }

    public List<SqlLarge> getSqls() {
        List<SqlLarge> list = new ArrayList<>(sqls);
        return list;
    }

    public void add(String sql, long sqlRows, long executeTime, long startTime, long endTime) {
    	SqlLarge sqlLarge = new SqlLarge(sql, sqlRows, executeTime, startTime, endTime);
    	this.add( sqlLarge );
    }
    
    public void add(SqlLarge sql) {
        sqls.add(sql);
    }
    
    public void reset() {
    	this.clear();
    }

    public void clear() {
        sqls.clear();
    }

    public void recycle() {
        if(sqls.size() > count){
            SortedSet<SqlLarge> sqls2 = new ConcurrentSkipListSet<>();
            List<SqlLarge> keyList = new ArrayList<SqlLarge>(sqls);
            int i = 0;
            for(SqlLarge key : keyList){
                if(i == count) {
                    break;
                }
                sqls2.add(key);
                i++;
            }
            sqls = sqls2;
        }
    }
    
    /**
     * 记录 SQL 及返回行数
     */
    public static class SqlLarge implements Comparable<SqlLarge> {
    	
    	private String sql;
    	private long sqlRows;
    	private long executeTime;
    	private long startTime;
    	private long endTime;
    	
		public SqlLarge(String sql, long sqlRows, long executeTime, long startTime, long endTime) {
			super();
			this.sql = sql;
			this.sqlRows = sqlRows;
			this.executeTime = executeTime;
			this.startTime = startTime;
			this.endTime = endTime;
		}

		public String getSql() {
			return sql;
		}

		public long getSqlRows() {
			return sqlRows;
		}

		public long getStartTime() {
			return startTime;
		}

		public long getExecuteTime() {
			return executeTime;
		}

		public long getEndTime() {
			return endTime;
		}
		
		@Override
	    public int compareTo(SqlLarge o) {
            long para = o.sqlRows - sqlRows;
	        return para == 0 ? (o.sql.hashCode() - sql.hashCode()) :(int) (para);
	    }

	    @Override
	    public boolean equals(Object arg0) {
	        return super.equals(arg0);
	    }

	    @Override
	    public int hashCode() {
	        return super.hashCode();
	    }
    }	
}