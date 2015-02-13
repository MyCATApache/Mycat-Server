package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.*;
import org.opencloudb.parser.druid.impl.*;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;

/**
 * DruidParser的工厂类
 * @author wdw
 *
 */
public class DruidParserFactory {
	
	public static DruidParser create(SQLStatement statement) {
		DruidParser parser;
		if(statement instanceof SQLSelectStatement) {
			parser = new DruidSelectParser();
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
		} else if (statement instanceof MySqlLockTableStatement) {
			parser = new DruidLockTableParser();
		} else if (statement instanceof MySqlUnlockTablesStatement) {
			parser = new DruidUnlockTablesParser();
		}  else {
			parser = new DefaultDruidParser();
		}
		
		return parser;
	}
}
