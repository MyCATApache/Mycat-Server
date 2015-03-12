package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.parser.druid.impl.*;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DruidParser的工厂类
 * @author wdw
 *
 */
public class DruidParserFactory {
	
	public static DruidParser create(SchemaConfig schema,SQLStatement statement,SchemaStatVisitor schemaStatVisitor) {
		DruidParser parser = null;
		if(statement instanceof SQLSelectStatement) {
			//先解出表，判断表所在db的类型，再根据不同db类型返回不同的解析
			List<String> tables=  parseTables(statement,schemaStatVisitor);
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


	private static List<String> parseTables(SQLStatement stmt,SchemaStatVisitor schemaStatVisitor)
	{
		List<String> tables=new ArrayList<>()  ;
		stmt.accept(schemaStatVisitor);

		if(schemaStatVisitor.getAliasMap() != null) {
			for(Map.Entry<String, String> entry : schemaStatVisitor.getAliasMap().entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				if(key != null && key.indexOf("`") >= 0) {
					key = key.replaceAll("`", "");
				}
				if(value != null && value.indexOf("`") >= 0) {
					value = value.replaceAll("`", "");
				}
				//表名前面带database的，去掉
				if(key != null) {
					int pos = key.indexOf(".");
					if(pos> 0) {
						key = key.substring(pos + 1);
					}
				}

				if(key.equals(value)) {
					tables.add(key.toUpperCase());
				}
			}

		}
	   return tables;
	}

}
