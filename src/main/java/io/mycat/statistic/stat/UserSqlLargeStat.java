package io.mycat.statistic.stat;

import java.util.Arrays;

public class UserSqlLargeStat {
	
	private int index;
    private long minValue;
    private final int count;
    private final int lastIndex;
    private final SqlLarge[] sqls;

    public UserSqlLargeStat(int count) {
        this.count = count;
        this.lastIndex = count - 1;
        this.sqls = new SqlLarge[count];
    }

    public SqlLarge[] getSqls() {    	
    	SqlLarge[] sqls2 = Arrays.copyOf(sqls, index);
    	Arrays.sort(sqls2);
        return sqls2;
    }

    /**
     * 检查当前的值能否进入排名
     */
    public boolean check(long value) {
        return (index < count) || (value > minValue);
    }

    public void add(String sql, long sqlRows, long executeTime, long startTime, long endTime) {
    	SqlLarge sqlLarge = new SqlLarge(sql, sqlRows, executeTime, startTime, endTime);
    	this.add( sqlLarge );
    }
    
    public void add(SqlLarge sql) {
		 if (index < count) {
	         sqls[index++] = sql;
	         if (index == count) {
	             Arrays.sort(sqls);
	             minValue = sqls[0].sqlRows;
	         }
	     } else {
	         swap(sql);
	     }
    }
    
    public void reset() {
    	this.clear();
    }

    public void clear() {
    	 for (int i = 0; i < count; i++) {
             sqls[i] = null;
         }
         index = 0;
         minValue = 0;
    }

    /**
     * 交换元素位置并重新定义最小值
     */
    private void swap(SqlLarge sql) {
        int x = find(sql.sqlRows, 0, lastIndex);
        switch (x) {
        case 0:
            break;
        case 1:
            minValue = sql.sqlRows;
            sqls[0] = sql;
            break;
        default:
            --x;// 向左移动一格
            final SqlLarge[] sqls = this.sqls;
            for (int i = 0; i < x; i++) {
                sqls[i] = sqls[i + 1];
            }
            sqls[x] = sql;
            minValue = sqls[0].sqlRows;
        }
    }

    /**
     * 定位v在当前范围内的排名
     */
    private int find(long v, int from, int to) {
        int x = from + ((to - from + 1) >> 1);
        if (v <= sqls[x].sqlRows) {
            --x;// 向左移动一格
            if (from >= x) {
                return v <= sqls[from].sqlRows ? from : from + 1;
            } else {
                return find(v, from, x);
            }
        } else {
            ++x;// 向右移动一格
            if (x >= to) {
                return v <= sqls[to].sqlRows ? to : to + 1;
            } else {
                return find(v, x, to);
            }
        }
    }
	
    
    /**
     * 记录 SQL 及返回行数
     */
    public static class SqlLarge implements Comparable<SqlLarge> {
    	
    	private String sql;
    	private long sqlRows;
    	private long executeTime;
    	private long startTime;
    	private long endTime;
    	
		public SqlLarge(String sql, long sqlRows, long executeTime, long startTime, long endTime) {
			super();
			this.sql = sql;
			this.sqlRows = sqlRows;
			this.executeTime = executeTime;
			this.startTime = startTime;
			this.endTime = endTime;
		}

		public String getSql() {
			return sql;
		}

		public long getSqlRows() {
			return sqlRows;
		}

		public long getStartTime() {
			return startTime;
		}

		public long getExecuteTime() {
			return executeTime;
		}

		public long getEndTime() {
			return endTime;
		}
		
		@Override
	    public int compareTo(SqlLarge o) {
	        return (int) (sqlRows - o.sqlRows);
	    }

	    @Override
	    public boolean equals(Object arg0) {
	        return super.equals(arg0);
	    }

	    @Override
	    public int hashCode() {
	        return super.hashCode();
	    }
    }	
}