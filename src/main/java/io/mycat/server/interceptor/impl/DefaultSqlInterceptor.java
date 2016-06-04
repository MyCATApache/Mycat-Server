package io.mycat.server.interceptor.impl;

import io.mycat.MycatServer;
import io.mycat.config.model.SystemConfig;
import io.mycat.server.interceptor.SQLInterceptor;

public class DefaultSqlInterceptor implements SQLInterceptor {
	private static final char ESCAPE_CHAR = '\\';

	private static final int TARGET_STRING_LENGTH = 2;

	/**
	 * mysql driver对'转义与\',解析前改为foundationdb parser支持的'' add by sky
	 * 
	 * @param sql
	 * @update by jason@dayima.com replace regex with general string walking
	 * avoid sql being destroyed in case of some mismatch
	 * maybe some performance enchanced
	 * @return
	 */
	public static String processEscape(String sql) {
		int firstIndex = -1;
		if ((sql == null) || ((firstIndex = sql.indexOf(ESCAPE_CHAR)) == -1)) {
			return sql;
		} else {
			int lastIndex = sql.lastIndexOf(ESCAPE_CHAR, sql.length() - 2) + TARGET_STRING_LENGTH;
			StringBuilder sb = new StringBuilder(sql);
			for (int i = firstIndex; i < lastIndex; i ++) {
				if (sb.charAt(i) == '\\') {
					if (i + 1 < lastIndex
							&& sb.charAt(i + 1) == '\'') {
							//replace
							sb.setCharAt(i, '\'');
					}
					//roll over
					i ++;
				}
			}
			return sb.toString();
		}
	}

	/**
	 * escape mysql escape letter sql type ServerParse.UPDATE,ServerParse.INSERT
	 * etc
	 */
	@Override
	public String interceptSQL(String sql, int sqlType) {
		if("fdbparser".equals(MycatServer.getInstance().getConfig().getSystem().getDefaultSqlParser())) {
			sql = processEscape(sql);
		}
		
		// 全局表一致性 sql 改写拦截
		SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
		if(system != null && system.getUseGlobleTableCheck() == 1) // 全局表一致性检测是否开启
			sql = GlobalTableUtil.interceptSQL(sql, sqlType);
		
		// other interceptors put in here ....
		
		return sql;
	}

}
