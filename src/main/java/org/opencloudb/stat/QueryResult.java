package org.opencloudb.stat;

/**
 * SQL 执行结果
 * 
 * @author zhuam
 *
 */
public class QueryResult {

	private String user;
	private int sqlType;
	private String sql;
	private long startTime;
	private long endTime;
	
	public QueryResult(String user, int sqlType, String sql, long startTime) {
		super();
		this.user = user;
		this.sqlType = sqlType;
		this.sql = sql;
		this.startTime = startTime;
	}

	public String getUser() {
		return user;
	}

	public int getSqlType() {
		return sqlType;
	}

	public String getSql() {
		return sql;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

}
