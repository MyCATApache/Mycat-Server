package io.mycat.statistic.stat;

import io.mycat.server.parser.ServerParse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SQL统计中,统计出来每个表的读,写的TPS,分辨出当前最热的表，
 * 并且根据是否有关联JOIN来区分为几个不同的“区域”,是一个重要功能,意味着,某些表可以转移到其他的数据库里,做智能优化。
 * 
 * 首先是每个表的读写TPS 2个指标,有时段。然后是 哪那些表有JOIN查询 ，来区分 独立的区域
 * 
 * @author zhuam
 *
 */
public class TableStat implements Comparable<TableStat> {
	
	//1、读写
	//2、主表
	//3、关联表  次数
	//4、读写 TPS
	
	public String table;

	private final AtomicLong rCount = new AtomicLong(0);
    private final AtomicLong wCount = new AtomicLong(0);
    
    // 关联表
    private final ConcurrentHashMap<String, RelaTable> relaTableMap = new ConcurrentHashMap<String, RelaTable>();    
    
    /**
     * 最后执行时间
     */
    private long lastExecuteTime;
    
    
    public TableStat(String table) {
		super();
		this.table = table;		
    }
    
	public void reset() {		
		this.rCount.set(0);
		this.wCount.set(0);		
		this.relaTableMap.clear();
		this.lastExecuteTime = 0;
	}
	
	public void update(int sqlType, String sql, long startTime, long endTime, List<String> relaTables) {
		
		//记录 RW
		switch(sqlType) {
    	case ServerParse.SELECT:		
			this.rCount.incrementAndGet();		
    		break;
    	case ServerParse.UPDATE:			
    	case ServerParse.INSERT:		
    	case ServerParse.DELETE:
    	case ServerParse.REPLACE:
    		this.wCount.incrementAndGet();
    		break;
    	}
		
		// 记录 关联表执行情况
		for(String table: relaTables) {
			RelaTable relaTable = this.relaTableMap.get( table );
			if ( relaTable == null ) {
				relaTable = new RelaTable(table, 1);
			} else {
				relaTable.incCount();
			}
			this.relaTableMap.put(table, relaTable);
		}
	
		this.lastExecuteTime = endTime;
	}
	
    public String getTable() {
		return table;
	}

	public long getRCount() {
        return this.rCount.get();
    }
    
    public long getWCount() {
        return this.wCount.get();
    }
    
	public int getCount() {
		return (int)(getRCount()+getWCount());
	}    
	
    public List<RelaTable> getRelaTables() {    	
    	List<RelaTable> tables = new ArrayList<RelaTable>();
    	tables.addAll( this.relaTableMap.values() );
    	return tables;
    }
    
    public long getLastExecuteTime() {
		return lastExecuteTime;
	}

	@Override
	public int compareTo(TableStat o) {
		long para = o.getCount() - getCount();
		long para2 = o.getLastExecuteTime() - getLastExecuteTime();
		return para == 0? (para2 == 0? o.getTable().hashCode() - getTable().hashCode() :(int) para2) : (int)para ;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TableStat) {
			return this.compareTo((TableStat)obj) == 0;
		} else {
			return super.equals(obj);
		}
	}

	/**
     * 关联表
     * @author Ben
     *
     */
    public static class RelaTable {
    	
    	private String tableName;
    	private int count;
    	
		public RelaTable(String tableName, int count) {
			super();
			this.tableName = tableName;
			this.count = count;
		}

		public String getTableName() {
			return this.tableName;
		}
		
		public int getCount() {
			return this.count;
		}
		
		public void incCount() {
			this.count++;
		}    	
    }
	
}
