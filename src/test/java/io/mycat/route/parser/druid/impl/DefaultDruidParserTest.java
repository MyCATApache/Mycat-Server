package io.mycat.route.parser.druid.impl;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.model.SchemaConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.parser.druid.DruidParser;
import io.mycat.route.parser.druid.DruidShardingParseInfo;
import io.mycat.route.parser.druid.MycatSchemaStatVisitor;
import io.mycat.server.parser.ServerParse;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * sql解析单元测试
 * @author lian
 * @date 2016年12月2日
 */
public class DefaultDruidParserTest {

	private SchemaConfig schema;
	private DruidParser druidParser;
	@Before
	public void setUp(){

		schema = mock(SchemaConfig.class);
		druidParser = new DefaultDruidParser();
	}
	@Test
	public void testParser() throws Exception {

		assertArrayEquals(getParseTables("select id as id from company t;"),
				getArr("company".toUpperCase()));
		assertArrayEquals(getParseTables("select 1 from (select 1 from company) company;"),
				getArr("company".toUpperCase()));
		assertArrayEquals(getParseTables("select 1 from company,customer where company.id = customer.cid"),
				getArr("company".toUpperCase(),"customer".toUpperCase()));
		assertArrayEquals(getParseTables("select 1 from db1.company,db1.customer where company.id = customer.cid"),
				getArr("company".toUpperCase(),"customer".toUpperCase()));
		assertArrayEquals(getParseTables("select 1 from db2.company,db1.customer where company.id = customer.cid"),
				getArr("company".toUpperCase(),"customer".toUpperCase()));
		assertArrayEquals(getParseTables("select 1 from mysql.company,db1.customer where company.id = customer.cid"),
				getArr());
	}

	private Object[] getParseTables(String sql) throws Exception{

		SQLStatementParser parser = new MySqlStatementParser(sql);
		SQLStatement statement = parser.parseStatement();
		MycatSchemaStatVisitor visitor = new MycatSchemaStatVisitor();


		LayerCachePool cachePool = mock(LayerCachePool.class);
		RouteResultset rrs = new RouteResultset(sql, ServerParse.SELECT);

		druidParser.parser(schema, rrs, statement, sql, cachePool, visitor);

		DruidShardingParseInfo ctx = druidParser.getCtx();
		return ctx.getTables().toArray();
	}

	private Object[] getArr(String...strings){
		return strings;
	}
}
