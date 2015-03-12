package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.dialect.sqlserver.ast.SQLServerSelect;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.parser.druid.impl.*;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DruidParser的工厂类
 * @author wdw
 *
 */
public class DruidParserFactory {
	
	public static DruidParser create(SchemaConfig schema,SQLStatement statement,List<String> tables) {
		DruidParser parser = null;
		if(statement instanceof SQLSelectStatement) {
			//先解出表，判断表所在db的类型，再根据不同db类型返回不同的解析
			for (String table : tables)
			{
				if (schema.getTables().get(table).getDbTypes().contains("oracle"))//if(schema.getAllDbTypeSet().contains("oracle")&&schema.isTableInThisDb(table,"oracle"))
				{
					parser=new DruidSelectOracleParser(); break;
				} else if(schema.getTables().get(table).getDbTypes().contains("db2"))
				{
					parser=new DruidSelectDb2Parser(); break;
				}
				else if(schema.getTables().get(table).getDbTypes().contains("sqlserver"))
				{
					parser=new DruidSelectSqlServerParser();  break;
				}
                else if(schema.getTables().get(table).getDbTypes().contains("postgresql"))
                {
                    parser=new DruidSelectPostgresqlParser();break;
                }
			}
            if(((SQLSelectStatement) statement).getSelect() instanceof SQLServerSelect)
            {
                parser=new DruidSelectSqlServerParser();
            }

			if(parser==null)
			{
			parser = new DruidSelectParser();
			}
		} else if(statement instanceof MySqlInsertStatement) {
			parser = new DruidInsertParser();
		} else if(statement instanceof MySqlDeleteStatement) {
			parser = new DruidDeleteParser();
		} else if(statement instanceof MySqlCreateTableStatement) {
			parser = new DruidCreateTableParser();
		} else if(statement instanceof MySqlUpdateStatement) {
			parser = new DruidUpdateParser();
		} else if(statement instanceof MySqlAlterTableStatement) {
			parser = new DruidAlterTableParser();
		} else {
			parser = new DefaultDruidParser();
		}
		
		return parser;
	}



}
