package org.opencloudb.parser;

import java.sql.SQLSyntaxErrorException;

import junit.framework.Assert;

import org.junit.Test;

import com.foundationdb.sql.parser.CallStatementNode;
import com.foundationdb.sql.parser.QueryTreeNode;

public class TestCallQLAnalyser
{

	@Test
	public void testCallSQL() throws SQLSyntaxErrorException
	{
		String sql = null;
		QueryTreeNode ast = null;
		
		sql = "call proc_rtn_list(10000)";
		ast = SQLParserDelegate.parse(sql, SQLParserDelegate.DEFAULT_CHARSET);
		
		Assert.assertTrue(ast instanceof CallStatementNode);
	}
}