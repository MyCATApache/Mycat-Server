package io.mycat.server.interceptor.impl;

import org.junit.Test;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import junit.framework.Assert;

public class GlobalTableUtilTest {
	
	private static final String originSql1 = "CREATE TABLE retl_mark"
			+ "("	
			+ "	ID BIGINT AUTO_INCREMENT,"
			+ "	CHANNEL_ID INT(11),"
			+ "	CHANNEL_INFO varchar(128),"
			+ "	CONSTRAINT RETL_MARK_ID PRIMARY KEY (ID)"
			+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
	
	private static final String originSql2 = "CREATE TABLE retl_mark"
			+ "("	
			+ "	ID BIGINT AUTO_INCREMENT,"
			+ "	CHANNEL_ID INT(11),"
			+ "	CHANNEL_INFO varchar(128),"
			+ " _MYCAT_OP_TIME int,"
			+ "	CONSTRAINT RETL_MARK_ID PRIMARY KEY (ID)"
			+ ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";
	
	@Test
	public void addColumnIfCreate() {
		String sql = parseSql(originSql1);
		System.out.println(sql);
		boolean contains = sql.contains("_mycat_op_time ");
		Assert.assertTrue(contains);
		sql = parseSql(originSql2);
		System.out.println(sql);
		Assert.assertFalse(sql.contains("_mycat_op_time int COMMENT '全局表保存修改时间戳的字段名'"));
	}

	public String parseSql(String sql) {
		MySqlStatementParser parser = new MySqlStatementParser(sql);
		SQLStatement statement = parser.parseStatement();
		return GlobalTableUtil.addColumnIfCreate(sql, statement);
	}

}
