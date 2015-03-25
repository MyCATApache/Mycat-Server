package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.config.model.TableConfig;
import org.opencloudb.parser.druid.impl.*;

import java.util.*;

/**
 * DruidParser的工厂类
 *
 * @author wdw
 */
public class DruidParserFactory
{

    public static DruidParser create(SchemaConfig schema, SQLStatement statement, SchemaStatVisitor visitor)
    {
        DruidParser parser = null;
        if (statement instanceof SQLSelectStatement)
        {
            if(schema.isNeedSupportMultiDBType())
            {
                parser = getDruidParserForMultiDB(schema, statement, visitor);

            }

            if (parser == null)
            {
                parser = new DruidSelectParser();
            }
        } else if (statement instanceof MySqlInsertStatement)
        {
            parser = new DruidInsertParser();
        } else if (statement instanceof MySqlDeleteStatement)
        {
            parser = new DruidDeleteParser();
        } else if (statement instanceof MySqlCreateTableStatement)
        {
            parser = new DruidCreateTableParser();
        } else if (statement instanceof MySqlUpdateStatement)
        {
            parser = new DruidUpdateParser();
        } else if (statement instanceof MySqlAlterTableStatement)
        {
            parser = new DruidAlterTableParser();
        } else
        {
            parser = new DefaultDruidParser();
        }

        return parser;
    }

    private static DruidParser getDruidParserForMultiDB(SchemaConfig schema, SQLStatement statement, SchemaStatVisitor visitor)
    {
        DruidParser parser=null;
        //先解出表，判断表所在db的类型，再根据不同db类型返回不同的解析
        List<String> tables = parseTables(statement, visitor);
        for (String table : tables)
        {
            Set<String> dbTypes =null;
            TableConfig tableConfig = schema.getTables().get(table);
            if(tableConfig==null)
            {
                dbTypes=new HashSet<>();
                dbTypes.add(schema.getDefaultDataNodeDbType())  ;
            }else
            {
                dbTypes = tableConfig.getDbTypes();
            }
            if (dbTypes.contains("oracle"))
            {
                parser = new DruidSelectOracleParser();
                break;
            } else if (dbTypes.contains("db2"))
            {
                parser = new DruidSelectDb2Parser();
                break;
            } else if (dbTypes.contains("sqlserver"))
            {
                parser = new DruidSelectSqlServerParser();
                break;
            } else if (dbTypes.contains("postgresql"))
            {
                parser = new DruidSelectPostgresqlParser();
                break;
            }
        }
        return parser;
    }


    private static List<String> parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor)
    {
        List<String> tables = new ArrayList<>();
        stmt.accept(schemaStatVisitor);

        if (schemaStatVisitor.getAliasMap() != null)
        {
            for (Map.Entry<String, String> entry : schemaStatVisitor.getAliasMap().entrySet())
            {
                String key = entry.getKey();
                String value = entry.getValue();
                if (key != null && key.indexOf("`") >= 0)
                {
                    key = key.replaceAll("`", "");
                }
                if (value != null && value.indexOf("`") >= 0)
                {
                    value = value.replaceAll("`", "");
                }
                //表名前面带database的，去掉
                if (key != null)
                {
                    int pos = key.indexOf(".");
                    if (pos > 0)
                    {
                        key = key.substring(pos + 1);
                    }
                }

                if (key.equals(value))
                {
                    tables.add(key.toUpperCase());
                }
            }

        }
        return tables;
    }


}
