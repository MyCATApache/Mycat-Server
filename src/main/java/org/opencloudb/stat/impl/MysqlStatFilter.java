package org.opencloudb.stat.impl;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Collections;

import org.opencloudb.stat.StatFilter;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.druid.stat.JdbcDataSourceStat;
import com.alibaba.druid.stat.JdbcSqlStat;
import com.alibaba.druid.stat.JdbcStatContext;
import com.alibaba.druid.stat.JdbcStatManager;
import com.alibaba.druid.util.JdbcSqlStatUtils;
import com.alibaba.druid.util.MapComparator;

public class MysqlStatFilter implements StatFilter {
	
    private final static int              DEFAULT_PAGE           = 1;
    private final static int              DEFAULT_PER_PAGE_COUNT = Integer.MAX_VALUE;
    private static final String           DEFAULT_ORDER_TYPE     = "asc";
    private static final String           DEFAULT_ORDERBY        = "SQL";
    
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
        try {
            sql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType);
        } catch (Exception e) {
          //  LOG.error("merge sql error, dbType " + dbType + ", sql : \n" + sql, e);
        }
		return sql;
	}
	
    public JdbcSqlStat createSqlStat(String sql) {
        JdbcStatContext context = JdbcStatManager.getInstance().getStatContext();
        String contextSql = context != null ? context.getSql() : null;
        if (contextSql != null && contextSql.length() > 0) {
            return updateSqlStat(contextSql);
            
        } else {
            String dbType = this.dbType;
            sql = mergeSql(sql, dbType);
            return updateSqlStat(sql);
        }
    }   
    public JdbcSqlStat updateSqlStat(String sql){
    	JdbcSqlStat sqlStat=dataSourceStat.createSqlStat(sql);
    	sqlStat.incrementExecuteSuccessCount();
    	sqlStat.addExecuteTime(System.currentTimeMillis());
    	sqlStat.setExecuteLastStartTime(System.currentTimeMillis());
    	return sqlStat;
    }
    public Map<String, JdbcSqlStat> getSqlStatMap() {
        return this.dataSourceStat.getSqlStatMap();
    }
    
    public List<Map<String, Object>> getSqlStatMap(Map parameters) {
    	List<Map<String, Object>> array = getSqlStatDataList();
        List<Map<String, Object>> sortedArray = comparatorOrderBy(array, parameters);
        return sortedArray;    	
    }    
    
    public List<Map<String, Object>> getSqlStatDataList() {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        Map<?, ?> sqlStatMap = this.dataSourceStat.getSqlStatMap();
        for (Object sqlStat : sqlStatMap.values()) {
            Map<String, Object> data = JdbcSqlStatUtils.getData(sqlStat);

            long executeCount = (Long) data.get("ExecuteCount");
            long runningCount = (Long) data.get("RunningCount");

            if (executeCount == 0 && runningCount == 0) {
                continue;
            }

            result.add(data);
        }

        return result;
    }    
    private List<Map<String, Object>> comparatorOrderBy(List<Map<String, Object>> array, Map<String, String> parameters) {
        // when open the stat page before executing some sql
        if (array == null || array.isEmpty()) {
            return null;
        }

        // when parameters is null
        String orderBy, orderType = null;
        Integer page = DEFAULT_PAGE;
        Integer perPageCount = DEFAULT_PER_PAGE_COUNT;
        if (parameters == null) {
            orderBy = DEFAULT_ORDER_TYPE;
            orderType = DEFAULT_ORDER_TYPE;
            page = DEFAULT_PAGE;
            perPageCount = DEFAULT_PER_PAGE_COUNT;
        } else {
            orderBy = parameters.get("orderBy");
            orderType = parameters.get("orderType");
            String pageParam = parameters.get("page");
            if (pageParam != null && pageParam.length() != 0) {
                page = Integer.parseInt(pageParam);
            }
            String pageCountParam = parameters.get("perPageCount");
            if (pageCountParam != null && pageCountParam.length() > 0) {
                perPageCount = Integer.parseInt(pageCountParam);
            }
        }

        // others,such as order
        orderBy = orderBy == null ? DEFAULT_ORDERBY : orderBy;
        orderType = orderType == null ? DEFAULT_ORDER_TYPE : orderType;

        if (!"desc".equals(orderType)) {
            orderType = DEFAULT_ORDER_TYPE;
        }

        // orderby the statData array
        if (orderBy != null && orderBy.trim().length() != 0) {
            Collections.sort(array, new MapComparator<String, Object>(orderBy, DEFAULT_ORDER_TYPE.equals(orderType)));
        }

        // page
        int fromIndex = (page - 1) * perPageCount;
        int toIndex = page * perPageCount;
        if (toIndex > array.size()) {
            toIndex = array.size();
        }

        return array.subList(fromIndex, toIndex);
    }    
}
