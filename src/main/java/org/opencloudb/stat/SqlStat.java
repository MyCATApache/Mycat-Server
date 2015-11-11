package org.opencloudb.stat;


import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sql 状态
 * 
 * @author zhuam
 *
 */
public class SqlStat {
	
	private int index;
	private final int count;
	private final Sql[] sqls;
	private final ReentrantLock lock;
	 
	public SqlStat(int count) {		
		this.index = 0;
        this.count = count;        
        this.sqls = new Sql[count];
        this.lock = new ReentrantLock();
	}

	public Sql[] getSqls() {		
		
		Sql[] newsqls = Arrays.copyOf(sqls, sqls.length);
		Arrays.sort(newsqls, new SqlComparator());
		return newsqls;
	}
	
	public void reset() {
		final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            for (int i = 0; i < count; i++) {
            	sqls[i] = null;
            }
            index = 0;
        } finally {
            lock.unlock();
        }
	}

    public void add(String sql, long startTime, long executeTime ) {
    	
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
        	Sql newSql = new Sql(sql, startTime, executeTime);
        	
            if (index < count) {
                sqls[index++] = newSql;
            } else {
            	index = 0;
            	sqls[index++] = newSql;
            }
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            for (int i = 0; i < count; i++) {
                sqls[i] = null;
            }
            index = 0;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 记录SQL
     */
    public class Sql {
    	
    	private String sql;
    	private long startTime;
    	private long executeTime;
    	
		public Sql(String sql, long startTime, long executeTime) {
			super();
			this.sql = sql;
			this.startTime = startTime;
			this.executeTime = executeTime;
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

    }
    
    public class SqlComparator implements Comparator<Sql> {
		@Override
		public int compare(Sql t1, Sql t2) {			
			long st1 = t1 == null ? 0 : t1.getStartTime();
			long st2 = t2 == null ? 0 : t2.getStartTime();
			
			return (int) (st1 - st2);
		}
    }

}
