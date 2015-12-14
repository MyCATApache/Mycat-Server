package org.opencloudb.stat;

import org.opencloudb.MycatServer;
import org.opencloudb.statistic.SQLRecord;
import org.opencloudb.statistic.SQLRecorder;

/**
 * 用户状态
 * 
 * @author Ben
 */
public class UserStat {
	
	//private final static int SQL_SLOW_TIME = 1000;
	private  long SQL_SLOW_TIME = 1000;
	/**
	 * SQL 执行记录
	 */
	private UserSqlStat sqlStat = null;
	
	/**
	 * CURD 执行分布
	 */
	private UserRWStat rwStat = null;
	
	/**
	 * 慢查询记录器  TOP 10
	 */
	private SQLRecorder sqlRecorder;

	private String user;
	

	
	public UserStat(String user) {
		super();
		this.user = user;		
		this.rwStat = new UserRWStat();
		this.sqlStat = new UserSqlStat(50);
		this.sqlRecorder =  new SQLRecorder(MycatServer.getInstance().getConfig().getSystem().getSqlRecordCount());
	}

	public String getUser() {
		return user;
	}

	public SQLRecorder getSqlRecorder() {
		return sqlRecorder;
	}

	public UserRWStat getRWStat() {
		return rwStat;
	}

	public UserSqlStat getSqlStat() {
		return sqlStat;
	}
	public void setSlowTime(long time) {
		this.SQL_SLOW_TIME=time;
	}
	public void reset() {		
		this.sqlRecorder.clear();
		this.rwStat.reset();
		this.sqlStat.reset();
	}

	/**
	 * 更新状态
	 * 
	 * @param sqlType
	 * @param sql
	 * @param startTime
	 */
	public void update(int sqlType, String sql, long startTime, long endTime) {	
		
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
		this.rwStat.add(sqlType, executeTime, startTime, endTime);
		
		//记录SQL
		this.sqlStat.add(sql, executeTime, startTime, endTime );
	}

}
