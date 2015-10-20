package org.opencloudb.interceptor.impl;

import org.opencloudb.interceptor.SQLInterceptor;
import org.opencloudb.stat.impl.MysqlStatFilter;

public class StatSqlInterceptor implements SQLInterceptor {

	@Override
	public String interceptSQL(String sql, int sqlType) {
		// TODO Auto-generated method stub
		final int atype = sqlType;
        final String sqls = DefaultSqlInterceptor.processEscape(sql);
        MysqlStatFilter.getInstance().createSqlStat(sqls);
        return sql;
	}

}
