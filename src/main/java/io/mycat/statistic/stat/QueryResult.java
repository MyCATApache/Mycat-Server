package io.mycat.statistic.stat;

/**
 * SQL 执行结果
 * 
 * @author zhuam
 *
 */
public class QueryResult {

	private String user;		//用户
	private int sqlType;		//SQL类型
	private String sql;			//SQL
	private long sqlRows;		//SQL 返回或影响的结果集长度
	private long netInBytes;	//NET IN 字节数
	private long netOutBytes;	//NET OUT 字节数
	private long startTime;		//开始时间
	private long endTime;		//结束时间
	private int resultSize;     //结果集大小
	private String host;

	public QueryResult(String user, int sqlType, String sql, long sqlRows,
                       long netInBytes, long netOutBytes, long startTime, long endTime
            , int resultSize, String host) {
		super();
		this.user = user;
		this.sqlType = sqlType;
		this.sql = sql;
		this.sqlRows = sqlRows;
		this.netInBytes = netInBytes;
		this.netOutBytes = netOutBytes;
		this.startTime = startTime;
		this.endTime = endTime;
		this.resultSize=resultSize;
		this.host = host;
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

	public long getSqlRows() {
		return sqlRows;
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
	
	public int getResultSize() {
		return resultSize;
	}

	public String getHost() {
		return host;
	}
}
