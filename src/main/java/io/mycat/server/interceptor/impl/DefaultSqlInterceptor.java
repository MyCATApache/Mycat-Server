package io.mycat.server.interceptor.impl;

import io.mycat.MycatServer;
import io.mycat.server.config.node.SystemConfig;
import io.mycat.server.interceptor.SQLInterceptor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultSqlInterceptor implements SQLInterceptor {
	private static final Pattern p = Pattern.compile("\\'", Pattern.LITERAL);

	private static final String TARGET_STRING = "''";

	private static final char ESCAPE_CHAR = '\\';

	private static final int TARGET_STRING_LENGTH = 2;

	/**
	 * mysql driver对'转义与\',解析前改为foundationdb parser支持的'' add by sky
	 * 
	 * @param stmt
	 * @return
	 */
	public static String processEscape(String sql) {
		int firstIndex = -1;
		if ((sql == null) || ((firstIndex = sql.indexOf(ESCAPE_CHAR)) == -1)) {
			return sql;
		} else {
			int lastIndex = sql.lastIndexOf(ESCAPE_CHAR, sql.length() - 2);// 不用考虑结尾字符为转义符
			Matcher matcher = p.matcher(sql.substring(firstIndex, lastIndex
					+ TARGET_STRING_LENGTH));
			String replacedStr = (lastIndex == firstIndex) ? matcher
					.replaceFirst(TARGET_STRING) : matcher
					.replaceAll(TARGET_STRING);
			StringBuilder sb = new StringBuilder(sql);
			sb.replace(firstIndex, lastIndex + TARGET_STRING_LENGTH,
					replacedStr);
			return sb.toString();
		}
	}

	/**
	 * escape mysql escape letter sql type ServerParse.UPDATE,ServerParse.INSERT
	 * etc
	 */
	@Override
	public String interceptSQL(String sql, int sqlType) {
		String result = processEscape(sql);
		// 全局表一致性 sql 改写拦截
		SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
		if(system != null && system.isGlobalTableCheckSwitchOn()) // 全局表一致性检测是否开启
			result = GlobalTableUtil.interceptSQL(result, sqlType);
		return result;
	}

}
