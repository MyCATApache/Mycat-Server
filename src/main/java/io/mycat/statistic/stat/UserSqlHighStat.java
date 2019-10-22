package io.mycat.statistic.stat;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserSqlHighStat {

  private static final int CAPACITY_SIZE = 1024;

  private ConcurrentHashMap<String, SqlFrequency> sqlFrequencyMap = new ConcurrentHashMap<>();

  private SqlParser sqlParser = new SqlParser();
  private static final Logger LOGGER = LoggerFactory.getLogger(UserSqlHighStat.class);

  public void addSql(String sql, long executeTime, long startTime, long endTime,String host) {
    String newSql = this.sqlParser.mergeSql(sql);
    if (newSql != null) {
      this.sqlFrequencyMap.compute(newSql,
          (s, sqlFrequency) -> {
            if (sqlFrequency == null) {
              sqlFrequency = new SqlFrequency();
              sqlFrequency.setSql(s);
            }
            sqlFrequency.setLastTime(endTime);
            sqlFrequency.incCount();
            sqlFrequency.setExecuteTime(executeTime);
            sqlFrequency.setHost(host);
            return sqlFrequency;
          });
    }
  }


  /**
   * 获取 SQL 访问频率
   */
  public List<SqlFrequency> getSqlFrequency(boolean isClear) {
    List<SqlFrequency> list = new ArrayList<>(this.sqlFrequencyMap.values());
    if (isClear) {
      clearSqlFrequency();
    }
    return list;
  }


  private void clearSqlFrequency() {
    sqlFrequencyMap.clear();
  }

  public void recycle() {
    if (sqlFrequencyMap.size() > CAPACITY_SIZE) {
      ConcurrentHashMap<String, SqlFrequency> sqlFrequencyMap2 = new ConcurrentHashMap<>();
      SortedSet<SqlFrequency> sqlFrequencySortedSet = new TreeSet<>(this.sqlFrequencyMap.values());
      List<SqlFrequency> keyList = new ArrayList<SqlFrequency>(sqlFrequencySortedSet);
      int i = 0;
      for (SqlFrequency key : keyList) {
        if (i == CAPACITY_SIZE) {
          break;
        }
        sqlFrequencyMap2.put(key.getSql(), key);
        i++;
      }
      sqlFrequencyMap = sqlFrequencyMap2;
    }
  }


  private static class SqlParser {

    public String fixSql(String sql) {
      if (sql != null) {
        return sql.replace("\n", " ");
      }
      return sql;
    }

    public String mergeSql(String sql) {
      try {
        String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, "mysql");
        return fixSql(newSql);
      } catch (Exception e) {
        LOGGER.warn("user sql high parse:{} error",sql, e);
        return null;
      }
    }

  }

}
