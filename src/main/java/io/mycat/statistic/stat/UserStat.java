package io.mycat.statistic.stat;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import io.mycat.MycatServer;
import io.mycat.server.parser.ServerParse;
import io.mycat.statistic.SQLRecord;
import io.mycat.statistic.SQLRecorder;

/**
 * 用户状态
 * 
 * @author Ben
 */
public class UserStat {
	
	private long SQL_SLOW_TIME = 100;
	
	private String user;
	
	/**
	 * 最大的并发
	 */
    private final AtomicInteger runningCount  = new AtomicInteger();
	private final AtomicInteger concurrentMax = new AtomicInteger();
	
	/**
	 * SQL 大集合插入、返回记录
	 */
	private UserSqlLargeStat sqlLargeStat = null;
	
	/**
	 * SQL 执行记录
	 */
	private UserSqlLastStat sqlLastStat = null;
	
	/**
	 * CURD 执行分布
	 */
	private UserSqlRWStat sqlRwStat = null;
	
	/**
	 * 用户高频SQL分析
	 */
	private UserSqlHighStat sqlHighStat = null;
	
	/**
	 * 慢查询记录器  TOP 10
	 */
	private SQLRecorder sqlRecorder;
	
	/**
	 * 大结果集记录
	 */
	private SqlResultSizeRecorder sqlResultSizeRecorder = null;
	
	/**
	 * 读写锁
	 */
//	private ReentrantReadWriteLock lock  = new ReentrantReadWriteLock();

	public UserStat(String user) {
		super();

		int size = MycatServer.getInstance().getConfig().getSystem().getSqlRecordCount();
		
		this.user = user;		
		this.sqlRwStat = new UserSqlRWStat();
		this.sqlLastStat = new UserSqlLastStat(50);
		this.sqlLargeStat = new UserSqlLargeStat(10);
		this.sqlHighStat = new UserSqlHighStat();		
		this.sqlRecorder = new SQLRecorder( size );
		this.sqlResultSizeRecorder =  new SqlResultSizeRecorder();
	}

	public String getUser() {
		return user;
	}

	public SQLRecorder getSqlRecorder() {
		return sqlRecorder;
	}

	public UserSqlRWStat getRWStat() {
		return sqlRwStat;
	}

	public UserSqlLastStat getSqlLastStat() {
		return this.sqlLastStat;
	}
	
	public UserSqlLargeStat getSqlLargeRowStat() {
		return this.sqlLargeStat;
	}
	
	public UserSqlHighStat getSqlHigh(){
		return this.sqlHighStat;
	}
	
	public SqlResultSizeRecorder getSqlResultSizeRecorder() {
		return this.sqlResultSizeRecorder;
	}
	
	
	public void setSlowTime(long time) {
		this.SQL_SLOW_TIME = time;
		this.sqlRecorder.clear();
	}
	
	public void clearSql() {
		this.sqlLastStat.reset();
	}
	
	public void clearSqlslow() {
		this.sqlRecorder.clear();
	}
	
	public void clearRwStat() {
		this.sqlRwStat.reset();
	}
	
	public void reset() {		
		this.sqlRecorder.clear();
		this.sqlResultSizeRecorder.clearSqlResultSet();
		this.sqlRwStat.reset();
		this.sqlLastStat.reset();
		
		this.runningCount.set(0);
		this.concurrentMax.set(0);
	}
	
	/**
	 * 更新状态
	 * 
	 * @param sqlType
	 * @param sql
	 * @param startTime
	 */
	public void update(int sqlType, String sql, long sqlRows, 
			long netInBytes, long netOutBytes, long startTime, long endTime ,int rseultSetSize) {	
		
		//before 计算最大并发数
		//-----------------------------------------------------
		int invoking = runningCount.incrementAndGet();
        for (;;) {
            int max = concurrentMax.get();
            if (invoking > max) {
                if (concurrentMax.compareAndSet(max, invoking)) {
                    break;
                }
            } else {
                break;
            }
        }
        //-----------------------------------------------------
		
//		this.lock.writeLock().lock();
//        try {
        	
			//慢查询记录
			long executeTime = endTime - startTime;		
			if ( executeTime >= SQL_SLOW_TIME ){			
				SQLRecord record = new SQLRecord();
				record.executeTime = executeTime;
				record.statement = sql;
				record.startTime = startTime;
				this.sqlRecorder.add(record);
			}
			
			//执行状态记录
			this.sqlRwStat.setConcurrentMax( concurrentMax.get() );
			this.sqlRwStat.add(sqlType, sql, executeTime, netInBytes, netOutBytes, startTime, endTime);
			
			//记录最新执行的SQL
			this.sqlLastStat.add(sql, executeTime, startTime, endTime );
			
			//记录高频SQL
			this.sqlHighStat.addSql(sql, executeTime, startTime, endTime);
			
			//记录SQL Select 返回超过 10000 行的 大结果集
			if ( sqlType == ServerParse.SELECT && sqlRows > 10000 ) {
				this.sqlLargeStat.add(sql, sqlRows, executeTime, startTime, endTime);
			}
			
			//记录超过阈值的大结果集sql
			if(rseultSetSize>=MycatServer.getInstance().getConfig().getSystem().getMaxResultSet()){
			    this.sqlResultSizeRecorder.addSql(sql, rseultSetSize);
			}
			
//        } finally {
//        	this.lock.writeLock().unlock();
//        }
        
		//after
		//-----------------------------------------------------
		runningCount.decrementAndGet();		
	}
}