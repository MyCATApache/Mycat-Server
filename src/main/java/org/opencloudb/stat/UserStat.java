package org.opencloudb.stat;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.statistic.SQLRecord;
import org.opencloudb.statistic.SQLRecorder;

/**
 * 用户状态
 * 
 * @author Ben
 */
public class UserStat {
	

	private static final Logger LOGGER = Logger.getLogger(UserStat.class);
	
	private final static int SQL_SLOW_TIME = 1000;
	
	/**
	 * SQL 执行记录
	 */
	private SqlStat sqlStat = null;
	
	/**
	 * CURD 执行分布
	 */
	private RWStat rwStat = null;
	
	/**
	 * 慢查询记录器  TOP 10
	 */
	private SQLRecorder sqlRecorder;
	
	private String user;
	
	public UserStat(String user) {
		super();
		this.user = user;		
		this.rwStat = new RWStat();
		this.sqlStat = new SqlStat(10);
		this.sqlRecorder =  new SQLRecorder(MycatServer.getInstance().getConfig().getSystem().getSqlRecordCount());
	}

	public String getUser() {
		return user;
	}

	public SQLRecorder getSqlRecorder() {
		return sqlRecorder;
	}

	public RWStat getRWStat() {
		return rwStat;
	}

	public SqlStat getSqlStat() {
		return sqlStat;
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
	public void update(int sqlType, String sql, long startTime) {	
		
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("System record stat: sqlType:" + sqlType + ", sql:" + sql + ", startTime:" + startTime);
		}
		
		long now = System.currentTimeMillis();
		
		//慢查询记录
		long executeTime = now - startTime;		
		if ( executeTime >= SQL_SLOW_TIME ){			
			SQLRecord record = new SQLRecord();
			record.executeTime = executeTime;
			record.statement = sql;
			record.startTime = startTime;
			
			this.sqlRecorder.add(record);
		}
		
		//执行状态记录
		this.rwStat.add(sqlType, executeTime, now);
		
		//记录SQL
		this.sqlStat.add(sql, startTime, executeTime );
	}

}
