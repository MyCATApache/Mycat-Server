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
public class UserSqlStat {
	
	private int index;
	private final int count;
	private final Sql[] sqls;
	private final ReentrantLock lock;
	 
	public UserSqlStat(int count) {		
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

    public void add(String sql, String ip,  long executeTime, long startTime, long endTime ) {
    	
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
        	Sql newSql = new Sql(sql,ip, executeTime, startTime, endTime);        	
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
    	private String ip;
    	private long executeTime;
    	private long startTime;
    	private long endTime;
    	
		public Sql(String sql,String ip, long executeTime, long startTime, long endTime) {
			super();
			this.sql = sql;
			this.ip = ip;
			this.executeTime = executeTime;
			this.startTime = startTime;
			this.endTime = endTime;
		}

		public String getSql() {
			return sql;
		}
		
		public String getIp() {
			return ip;
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
    
    public class SqlComparator implements Comparator<Sql> {
		@Override
		public int compare(Sql t1, Sql t2) {			
			long st1 = t1 == null ? 0 : t1.getStartTime();
			long st2 = t2 == null ? 0 : t2.getStartTime();
			
			return (int) (st1 - st2);
		}
    }

}
