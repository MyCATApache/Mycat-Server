package org.opencloudb.stat;

public interface StatFilter {
    boolean isMergeSql();
    
    void setMergeSql(boolean mergeSql);
    
    boolean isLogSlowSql();
    
    void setLogSlowSql(boolean logSlowSql);
    
    String mergeSql(String sql, String dbType);
    
    long getSlowSqlMillis();
    
    void setSlowSqlMillis(long slowSqlMillis);
}