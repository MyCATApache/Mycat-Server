package io.mycat.statistic.stat;

import java.util.concurrent.atomic.AtomicLong;

public class SqlFrequency implements Comparable<SqlFrequency>{
	private String sql;
	private AtomicLong count = new AtomicLong(0);
	private long lastTime = 0;
	private long executeTime = 0;
	private long allExecuteTime = 0;
	private long maxTime = 0;
	private long avgTime = 0;
	private long minTime = 0;
	
	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public long getCount() {
		return this.count.get();
	}

	public void incCount() {
		this.count.getAndIncrement();
	}

	public long getLastTime() {
		return lastTime;
	}
	
	public void setLastTime(long lastTime) {
		this.lastTime = lastTime;
	}
	
	public long getExecuteTime() {
		return executeTime;
	}
	
	public long getMaxTime() {
		return maxTime;
	}
	
	public long getMinTime() {
		return minTime;
	}
	
	public long getAvgTime() {
		return avgTime;
	}	
	
	public void setExecuteTime(long execTime) {
		if (execTime > this.maxTime) {
			this.maxTime = execTime;
		}
		if (this.minTime == 0) {
			this.minTime = execTime;
		}
		if (execTime > 0
				&& execTime < this.minTime) {
				this.minTime = execTime;
		}
		this.allExecuteTime+=execTime;
		if (count.get() > 0) {
			this.avgTime = this.allExecuteTime / this.count.get();
		}		
		this.executeTime = execTime;
	}

	@Override
	public int compareTo(SqlFrequency o) {
		long para = o.count.get() - count.get();
		long para2 = o.lastTime - lastTime;
		return  para == 0L ? (int)(para2 == 0L ? o.allExecuteTime - allExecuteTime : para2) : (int)para ;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof SqlFrequency) {
			return this.compareTo((SqlFrequency)obj) == 0;
		} else {
			return super.equals(obj);
		}
	}
}