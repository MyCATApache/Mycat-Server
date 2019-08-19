package io.mycat.statistic.stat;

import io.mycat.server.parser.ServerParse;

import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL R/W 执行状态
 * 因为这里的所有元素都近似为非必须原子更新的，即：
 * 例如：rCount和netInBytes没有必要非得保持同步更新，在同一个事务内
 * 只要最后更新了即可，所以将其中的元素拆成一个一个原子类，没必要保证精确的保持原样不加任何锁
 * 
 * @author zhuam
 *
 */
public class UserSqlRWStat {
	

	/**
	 * R/W 次数
	 */
	private AtomicLong rCount = new AtomicLong(0L);
    private AtomicLong wCount = new AtomicLong(0L);
    
    /**
     * 每秒QPS
     */
    private int qps = 0;
    
    /**
     * Net In/Out 字节数
     */
    private AtomicLong netInBytes = new AtomicLong(0L);
    private AtomicLong netOutBytes = new AtomicLong(0L);
    
	/**
	 * 最大的并发
	 */
    private int concurrentMax = 1;
	
    /**
     * 执行耗时
     * 
     * 10毫秒内、 10 - 200毫秒内、 1秒内、 超过 1秒
     */
	private final Histogram timeHistogram = new Histogram( new long[] { 10, 200, 1000, 2000 } );
	
	/**
	 * 执行所在时段
	 * 
	 * 22-06 夜间、 06-13 上午、 13-18下午、 18-22 晚间
	 */
	private final Histogram executeHistogram = new Histogram(new long[] { 6, 13, 18, 22 });

    /**
     * 最后执行时间
	 * 不用很精确，所以不加任何锁
     */
    private long lastExecuteTime;
    
	
	private int time_zone_offset = 0;
	private int one_hour = 3600 * 1000;
	
	public UserSqlRWStat() {
		this.time_zone_offset = TimeZone.getDefault().getRawOffset();
	}
	
	public void reset() {
		this.rCount = new AtomicLong(0L);
		this.wCount = new AtomicLong(0L);
		this.concurrentMax = 1;		
		this.lastExecuteTime = 0;
		this.netInBytes = new AtomicLong(0L);
		this.netOutBytes = new AtomicLong(0L);
		
		this.timeHistogram.reset();
		this.executeHistogram.reset();
	}
	
	public void add(int sqlType, String sql, long executeTime, long netInBytes, long netOutBytes, long startTime, long endTime) {
		
	
		switch(sqlType) {
    	case ServerParse.SELECT:
    	case ServerParse.SHOW:
    		this.rCount.incrementAndGet();
    		break;
    	case ServerParse.UPDATE:
    	case ServerParse.INSERT:
    	case ServerParse.DELETE:
    	case ServerParse.REPLACE:
    		this.wCount.incrementAndGet();
    		break;
    	}
    	
    	//SQL执行所在的耗时区间
    	if ( executeTime <= 10 ) {
    		this.timeHistogram.record(10);
    		
    	} else if ( executeTime > 10 && executeTime <= 200 ) {
    		this.timeHistogram.record(200);
    		
    	} else if ( executeTime > 200 && executeTime <= 1000 ) {
    		this.timeHistogram.record(1000);
    		
    	} else if ( executeTime > 1000) {
    		this.timeHistogram.record(2000);
    	}
    	
    	//SQL执行所在的时间区间	
		long hour0 = endTime / ( 24L * (long)one_hour ) * ( 24L * (long)one_hour )- (long)time_zone_offset;
		long hour06 = hour0 + 6L * (long)one_hour - 1L;
		long hour13 = hour0 + 13L * (long)one_hour - 1L;
		long hour18 = hour0 + 18L * (long)one_hour - 1L;
		long hour22 = hour0 + 22L * (long)one_hour - 1L;
		
		if ( endTime <= hour06 || endTime > hour22 ) {
			this.executeHistogram.record(6);
			
		} else if ( endTime > hour06 && endTime <= hour13 ) {
			this.executeHistogram.record(13);
			
		} else if ( endTime > hour13 && endTime <= hour18 ) {
			this.executeHistogram.record(18);
			
		} else if ( endTime > hour18 && endTime <= hour22 ) {
			this.executeHistogram.record(22);	
		}		
		
		this.lastExecuteTime = endTime;
		
		this.netInBytes.addAndGet(netInBytes);
		this.netOutBytes.addAndGet(netOutBytes);
	}
	
    public long getLastExecuteTime() {
        return lastExecuteTime;
    }	
    
    public long getNetInBytes() {
    	return netInBytes.get();
    }
    
    public long getNetOutBytes() {
    	return netOutBytes.get();
    }

	public int getConcurrentMax() {
        return concurrentMax;
    }
	
	public void setConcurrentMax(int concurrentMax) {
		this.concurrentMax = concurrentMax;
	}
	
    public int getQps() {
		return qps;
	}

	public void setQps(int qps) {
		this.qps = qps;
	}

	public long getRCount() {
        return this.rCount.get();
    }
    
    public long getWCount() {
        return this.wCount.get();
    }
	
	public Histogram getTimeHistogram() {
        return this.timeHistogram;
    }
	
	public Histogram getExecuteHistogram() {
        return this.executeHistogram;
    }

}
