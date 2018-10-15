package io.mycat.route.parser.druid;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.route.parser.druid.impl.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DruidParser的工厂类
 *
 * @author wdw
 */
public class DruidParserFactory {

    /**
     * 根据数据库类型、SQL语句创建对应的Druid解析器
     * @param schema
     * @param statement
     * @return
     */
    public static DruidParser create(SchemaConfig schema, SQLStatement statement, SchemaStatVisitor visitor) {
        DruidParser parser = null;
        if (statement instanceof SQLSelectStatement) {
            // SQL Select 语句
            if(schema.isNeedSupportMultiDBType()) {
				// 支持多数据库类型
                parser = getDruidParserForMultiDB(schema, statement, visitor);

            }
            if (parser == null) { // 没有找到匹配的（MySQL） 都用 io.mycat.route.parser.druid.impl.DruidSelectParser 解析
                parser = new DruidSelectParser();
            }
        } else if (statement instanceof MySqlInsertStatement) {
            // MySQL Insert 语句
            parser = new DruidInsertParser();
        } else if (statement instanceof MySqlDeleteStatement) {
            // SQL Delete 语句
            parser = new DruidDeleteParser();
        } else if (statement instanceof MySqlUpdateStatement) {
            // MySQL Update 语句
            parser = new DruidUpdateParser();
        } else if (statement instanceof MySqlCreateTableStatement) {
            // SQL 创建表语句
            parser = new DruidCreateTableParser();
        } else if (statement instanceof SQLAlterTableStatement) {
            // MySQL 修改表语句
            parser = new DruidAlterTableParser();
        } else if (statement instanceof MySqlLockTableStatement) {
            // MySQL 锁表语句
        	parser = new DruidLockTableParser();
        } else {
            // 其他语句 使用默认 Druid 解析
            parser = new DefaultDruidParser();
        }
        return parser;
    }

    /**
     * 为多数据库获取合适的DruidParser
     * @param schema
     * @param statement
     * @return
     */
	private static DruidParser getDruidParserForMultiDB(SchemaConfig schema, SQLStatement statement, SchemaStatVisitor visitor) {
        DruidParser parser=null;
        //先解出表，判断表所在db的类型，再根据不同db类型返回不同的解析
        /**
         * 不能直接使用visitor变量，防止污染后续sql解析
         * @author SvenAugustus
         */
        SchemaStatVisitor _visitor = SchemaStatVisitorFactory.create(schema);
        List<String> tables = parseTables(statement, _visitor);
        for (String table : tables) {
            Set<String> dbTypes = null;
            TableConfig tableConfig = schema.getTables().get(table);
            if(tableConfig==null) {
                dbTypes=new HashSet<>();
                dbTypes.add(schema.getDefaultDataNodeDbType())  ;
            }else {
                dbTypes = tableConfig.getDbTypes();
            }
            if (dbTypes.contains("oracle")) {
                parser = new DruidSelectOracleParser();
                ((DruidSelectOracleParser)parser).setInvocationHandler(SqlMethodInvocationHandlerFactory.getForOracle());
                break;
            } else if (dbTypes.contains("db2")) {
                parser = new DruidSelectDb2Parser();
                break;
            } else if (dbTypes.contains("sqlserver")) {
                parser = new DruidSelectSqlServerParser();
                break;
            } else if (dbTypes.contains("postgresql")) {
                parser = new DruidSelectPostgresqlParser();
                ((DruidSelectPostgresqlParser)parser).setInvocationHandler(SqlMethodInvocationHandlerFactory.getForPgsql());
                break;
            }
        }
        return parser;
    }

    /**
     * 获取SQL语句中的表名
     * @param stmt
     * @param schemaStatVisitor
     * @return
     */
    private static List<String> parseTables(SQLStatement stmt, SchemaStatVisitor schemaStatVisitor) {
        List<String> tables = new ArrayList<>();
        stmt.accept(schemaStatVisitor);

        if (schemaStatVisitor.getTables() != null) { // 获取别名映射 包含表名的别名和字段名的别名
            for (Map.Entry<TableStat.Name, TableStat> entry : schemaStatVisitor.getTables().entrySet()) {
                String key = entry.getKey().getName();
                //表名前面带database的，去掉
                if (key != null) {
                    int pos = key.indexOf("`");
                    if (pos > 0) {
                        key = key.replaceAll("`", "");
                    }
                    pos = key.indexOf(".");
                    if (pos > 0) {
                        key = key.substring(pos + 1);
                    }
                    tables.add(key.toUpperCase()); // 表名转为大写
                }
            }
        }
        return tables;
    }
}
