package io.mycat.statistic.stat;

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
	private long netInBytes;
	private long netOutBytes;
	private long startTime;
	private long endTime;
	
	public QueryResult(String user, int sqlType, String sql, long netInBytes, long netOutBytes, long startTime, long endTime) {
		super();
		this.user = user;
		this.sqlType = sqlType;
		this.sql = sql;
		this.netInBytes = netInBytes;
		this.netOutBytes = netOutBytes;
		this.startTime = startTime;
		this.endTime = endTime;
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
	
	public long getNetInBytes() {
		return netInBytes;
	}

	public long getNetOutBytes() {
		return netOutBytes;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}
}
