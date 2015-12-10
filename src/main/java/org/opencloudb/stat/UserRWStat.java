package org.opencloudb.stat;

import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.opencloudb.server.parser.ServerParse;

/**
 * SQL R/W 执行状态
 * 
 * @author zhuam
 *
 */
public class UserRWStat {
	
	/**
	 * R/W 次数
	 */
	private final AtomicLong rCount = new AtomicLong(0);
    private final AtomicLong wCount = new AtomicLong(0);
    
    /**
     * 最后执行时间
     */
    private long lastExecuteTime;
    
	/**
	 * 最大的并发
	 */
    private final AtomicInteger runningCount  = new AtomicInteger();
	private final AtomicInteger concurrentMax = new AtomicInteger();
	
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
	
	private int time_zone_offset = 0;
	private int one_hour = 3600 * 1000;
	
	public UserRWStat() {
		this.time_zone_offset = TimeZone.getDefault().getRawOffset();
	}
	
	public void reset() {
		this.rCount.set(0);
		this.wCount.set(0);
		this.runningCount.set(0);
		this.concurrentMax.set(0);
		this.lastExecuteTime = 0;
		
		this.timeHistogram.reset();
		this.executeHistogram.reset();
	}
	
	public void add(int sqlType, long executeTime, long startTime, long endTime) {
		
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
		long hour0 = endTime / ( 24 * one_hour ) * ( 24 * one_hour )- time_zone_offset;
		long hour06 = hour0 + 6 * one_hour - 1; 
		long hour13 = hour0 + 13 * one_hour - 1; 
		long hour18 = hour0 + 18 * one_hour - 1;
		long hour22 = hour0 + 22 * one_hour - 1; 
		
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
		
		//after
		//-----------------------------------------------------
		runningCount.decrementAndGet();		
	}
	
    public long getLastExecuteTime() {
        return lastExecuteTime;
    }	
    
    public AtomicInteger getRunningCount() {
		return runningCount;
	}

	public int getConcurrentMax() {
        return concurrentMax.get();
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
