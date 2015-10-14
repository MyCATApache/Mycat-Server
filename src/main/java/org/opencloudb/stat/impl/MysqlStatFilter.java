package org.opencloudb.stat.impl;

import java.util.Map;

import org.opencloudb.stat.StatFilter;

import com.alibaba.druid.stat.JdbcDataSourceStat;
import com.alibaba.druid.stat.JdbcSqlStat;
import com.alibaba.druid.stat.JdbcStatContext;
import com.alibaba.druid.stat.JdbcStatManager;

public class MysqlStatFilter implements StatFilter {
	
    private boolean                   connectionStackTraceEnable = false;
    // 3 seconds is slow sql
    protected long                    slowSqlMillis              = 3 * 1000;
    protected boolean                 logSlowSql                 = false;
    private String                    dbType;
    private boolean                   mergeSql                   = false;
    
    private final JdbcDataSourceStat    dataSourceStat;
    private final static MysqlStatFilter     instance       = new MysqlStatFilter();
    
    public MysqlStatFilter() {    
       this.dataSourceStat = new JdbcDataSourceStat("","","mysql");//config.getName(), config.getUrl(), dbType);
    }
    
    public static MysqlStatFilter getInstance() {
        return instance;
    }    
    
    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public long getSlowSqlMillis() {
        return slowSqlMillis;
    }

    public void setSlowSqlMillis(long slowSqlMillis) {
        this.slowSqlMillis = slowSqlMillis;
    }

    public boolean isLogSlowSql() {
        return logSlowSql;
    }

    public void setLogSlowSql(boolean logSlowSql) {
        this.logSlowSql = logSlowSql;
    }

    public boolean isConnectionStackTraceEnable() {
        return connectionStackTraceEnable;
    }

    public void setConnectionStackTraceEnable(boolean connectionStackTraceEnable) {
        this.connectionStackTraceEnable = connectionStackTraceEnable;
    }

    public boolean isMergeSql() {
        return mergeSql;
    }

    public void setMergeSql(boolean mergeSql) {
        this.mergeSql = mergeSql;
    }

	@Override
	public String mergeSql(String sql, String dbType) {
		// TODO Auto-generated method stub
		return sql;
	}
	
    public JdbcSqlStat createSqlStat(String sql) {
        JdbcStatContext context = JdbcStatManager.getInstance().getStatContext();
        String contextSql = context != null ? context.getSql() : null;
        if (contextSql != null && contextSql.length() > 0) {
            return dataSourceStat.createSqlStat(contextSql);
        } else {
            String dbType = this.dbType;

           // if (dbType == null) {
           //     dbType = dataSource.getDbType();
           // }
            sql = mergeSql(sql, dbType);
            return dataSourceStat.createSqlStat(sql);
        }
    }   
    
    public Map<String, JdbcSqlStat> getSqlStatMap() {
        return this.dataSourceStat.getSqlStatMap();
    }    
}
