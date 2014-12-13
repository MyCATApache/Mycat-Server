package org.opencloudb.parser.druid;

import org.opencloudb.parser.druid.impl.DefaultDruidParser;
import org.opencloudb.parser.druid.impl.DruidAlterTableParser;
import org.opencloudb.parser.druid.impl.DruidCreateTableParser;
import org.opencloudb.parser.druid.impl.DruidDeleteParser;
import org.opencloudb.parser.druid.impl.DruidInsertParser;
import org.opencloudb.parser.druid.impl.DruidSelectParser;
import org.opencloudb.parser.druid.impl.DruidUpdateParser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

/**
 * DruidParser的工厂类
 * @author wdw
 *
 */
public class DruidParserFactory {
	
	public static DruidParser create(SQLStatement statement) {
		DruidParser parser = null;
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
		} else {
			parser = new DefaultDruidParser();
		}
		
		return parser;
	}
}
