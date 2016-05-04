package io.mycat.statistic.stat;

import java.util.Arrays;
import java.util.Comparator;

/**
 * 最后执行的 Sql
 * 
 * @author zhuam
 *
 */
public class UserSqlLastStat {
	
	private int index;
	private final int count;
	private final SqlLast[] sqls;
	 
	public UserSqlLastStat(int count) {		
		this.index = 0;
        this.count = count;        
        this.sqls = new SqlLast[count];
	}

	public SqlLast[] getSqls() {		
		SqlLast[] sql2 = Arrays.copyOf(sqls, index);
		Arrays.sort(sql2, new SqlComparator());
		return sql2;
	}

    public void add(String sql,  long executeTime, long startTime, long endTime ) {    	
    	SqlLast sqlLast = new SqlLast(sql, executeTime, startTime, endTime);        	
        if (index < count) {
            sqls[index++] = sqlLast;
        } else {
        	index = 0;
        	sqls[index++] = sqlLast;
        }
    }

    public void reset() {
    	this.clear();
    }
    
    public void clear() {
    	for (int i = 0; i < count; i++) {
            sqls[i] = null;
        }
        index = 0;
    }
    
    /**
     * 记录SQL
     */
    public class SqlLast {
    	
    	private String sql;
    	private long executeTime;
    	private long startTime;
    	private long endTime;
    	
		public SqlLast(String sql, long executeTime, long startTime, long endTime) {
			super();
			this.sql = sql;
			this.executeTime = executeTime;
			this.startTime = startTime;
			this.endTime = endTime;
		}

		public String getSql() {
			return sql;
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

    }
    
    public class SqlComparator implements Comparator<SqlLast> {
		@Override
		public int compare(SqlLast t1, SqlLast t2) {			
			long st1 = t1 == null ? 0 : t1.getStartTime();
			long st2 = t2 == null ? 0 : t2.getStartTime();
			
			return (int) (st1 - st2);
		}
    }

}