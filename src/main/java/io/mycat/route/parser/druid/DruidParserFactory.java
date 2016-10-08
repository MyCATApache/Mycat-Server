package io.mycat.route.parser.druid;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;

import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.parser.druid.impl.DefaultDruidParser;
import io.mycat.route.parser.druid.impl.DruidAlterTableParser;
import io.mycat.route.parser.druid.impl.DruidCreateTableParser;
import io.mycat.route.parser.druid.impl.DruidDeleteParser;
import io.mycat.route.parser.druid.impl.DruidInsertParser;
import io.mycat.route.parser.druid.impl.DruidLockTableParser;
import io.mycat.route.parser.druid.impl.DruidSelectDb2Parser;
import io.mycat.route.parser.druid.impl.DruidSelectOracleParser;
import io.mycat.route.parser.druid.impl.DruidSelectParser;
import io.mycat.route.parser.druid.impl.DruidSelectPostgresqlParser;
import io.mycat.route.parser.druid.impl.DruidSelectSqlServerParser;
import io.mycat.route.parser.druid.impl.DruidUpdateParser;

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
        } else if (statement instanceof SQLAlterTableStatement)
        {
            parser = new DruidAlterTableParser();
        } else if (statement instanceof MySqlLockTableStatement) {
        	parser = new DruidLockTableParser();
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
                if (value != null && value.indexOf("`") >= 0)
                {
                    value = value.replaceAll("`", "");
                }
                //表名前面带database的，去掉
                if (key != null)
                {
                    int pos = key.indexOf("`");
                    if (pos > 0)
                    {
                        key = key.replaceAll("`", "");
                    }
                    pos = key.indexOf(".");
                    if (pos > 0)
                    {
                        key = key.substring(pos + 1);
                    }

                    if (key.equals(value))
                    {
                        tables.add(key.toUpperCase());
                    }
                }
            }

        }
        return tables;
    }


}
