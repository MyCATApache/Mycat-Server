package org.opencloudb.stat;

public class SqlFrequency {
	private String sql;
	private int count = 0;
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

	public int getCount() {
		return count;
	}

	public void incCount() {
		this.count++;
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
		if (execTime > 0) {
			if (execTime < this.minTime) {
				this.minTime = execTime;
			}
		}
		this.allExecuteTime+=execTime;
		if (count > 0) {
			this.avgTime = this.allExecuteTime / count;
		}		
		this.executeTime = execTime;
	}		
}
