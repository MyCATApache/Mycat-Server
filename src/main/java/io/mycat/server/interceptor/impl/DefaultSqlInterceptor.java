package io.mycat.server.interceptor.impl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.mycat.MycatServer;
import io.mycat.server.interceptor.SQLInterceptor;

public class DefaultSqlInterceptor implements SQLInterceptor {
	//private static final Pattern p = Pattern.compile("\\'", Pattern.LITERAL);
	// fix bug: 无法在字符串的末尾插入 \ 字符的问题. by: digdeep@126.com
	private static final Pattern p = Pattern.compile("[^\\]\\'", Pattern.LITERAL);

	private static final String TARGET_STRING = "''";

	private static final char ESCAPE_CHAR = '\\';

	private static final int TARGET_STRING_LENGTH = 2;

	/**
	 * mysql driver对'转义与\',解析前改为foundationdb parser支持的'' add by sky
	 * 
	 * @param stmt
	 * @return
	 * @note: 该函数会导致无法正确的插入 \ 字符的问题：
	 * update travelrecord set name='test\\' where id=1;
	 * 插入的name的值会变成：test' 而不是我们期望的 test\
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
		if("fdbparser".equals(MycatServer.getInstance().getConfig().getSystem().getDefaultSqlParser()))
			sql = processEscape(sql);
		
		// other interceptors put in here ....
		
		return sql;
	}

}
