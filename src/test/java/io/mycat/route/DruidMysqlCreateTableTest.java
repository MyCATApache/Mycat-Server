package io.mycat.route;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import io.mycat.SimpleCachePool;
import io.mycat.cache.LayerCachePool;
import io.mycat.config.loader.SchemaLoader;
import io.mycat.config.loader.xml.XMLSchemaLoader;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.route.factory.RouteStrategyFactory;
import io.mycat.server.interceptor.impl.GlobalTableUtil;
import io.mycat.util.StringUtil;
import junit.framework.Assert;
import org.junit.Test;

import java.sql.SQLNonTransientException;
import java.util.List;
import java.util.Map;

public class DruidMysqlCreateTableTest
{
	protected Map<String, SchemaConfig> schemaMap;
	protected LayerCachePool cachePool = new SimpleCachePool();
    protected RouteStrategy routeStrategy;
    private static final String originSql1 = "CREATE TABLE autoslot"
            + "("
            + "	ID BIGINT AUTO_INCREMENT,"
            + "	CHANNEL_ID INT(11),"
            + "	CHANNEL_INFO varchar(128),"
            + "	CONSTRAINT RETL_MARK_ID PRIMARY KEY (ID)"
            + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;";


    public DruidMysqlCreateTableTest() {
		String schemaFile = "/route/schema.xml";
		String ruleFile = "/route/rule.xml";
		SchemaLoader schemaLoader = new XMLSchemaLoader(schemaFile, ruleFile);
		schemaMap = schemaLoader.getSchemas();
        RouteStrategyFactory.init();
        routeStrategy = RouteStrategyFactory.getRouteStrategy("druidparser");
	}

	@Test
	public void testCreate() throws SQLNonTransientException {

		SchemaConfig schema = schemaMap.get("mysqldb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, originSql1, null,
                null, cachePool);
        Assert.assertEquals(2, rrs.getNodes().length);
      String sql=  rrs.getNodes()[0].getStatement();

        Assert.assertTrue(parseSql(sql));


		


	}

   // @Test
    public void testInsert() throws SQLNonTransientException {

        SchemaConfig schema = schemaMap.get("mysqldb");
        RouteResultset rrs = routeStrategy.route(new SystemConfig(), schema, -1, "insert into autoslot (id,sid) values(1,2) ", null,
                null, cachePool);
        Assert.assertEquals(1, rrs.getNodes().length);

        Assert.assertTrue(isInsertHasSlot(rrs.getStatement()));





    }

    private boolean isInsertHasSlot(String sql)
    {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        MySqlInsertStatement insertStatement= (MySqlInsertStatement)parser.parseStatement();
     List<SQLExpr> cc= insertStatement.getColumns();
        for (SQLExpr sqlExpr : cc) {
            SQLIdentifierExpr c= (SQLIdentifierExpr) sqlExpr;
            if("_slot".equalsIgnoreCase(c.getName())   &&cc.size()==insertStatement.getValues().getValues().size())    return true;
        }
        return false;
    }

    public boolean parseSql(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLStatement statement = parser.parseStatement();
        return hasColumn(statement);
    }

    private static boolean hasColumn(SQLStatement statement){
        for (SQLTableElement tableElement : ((SQLCreateTableStatement)statement).getTableElementList()) {
            SQLName sqlName = null;
            if (tableElement instanceof SQLColumnDefinition) {
                sqlName = ((SQLColumnDefinition)tableElement).getName();
            }
            if (sqlName != null) {
                String simpleName = sqlName.getSimpleName();
                simpleName = StringUtil.removeBackquote(simpleName);
                if (tableElement instanceof SQLColumnDefinition && "_slot".equalsIgnoreCase(simpleName)) {
                    return true;
                }
            }
        }
        return false;
    }
}
