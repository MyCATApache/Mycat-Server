package io.mycat.backend.jdbc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class InsertSqlBinary4PgRewriter extends AbstractSqlBinary4PgRewriter {
	@Override
	public String rewrite(Connection con, String origSQL, String charset) throws SQLException {
		SQLStatement sqlStatement = null;

		//尝试以MySQL讲法解析语句,如何解析通过,则进行有关"X'"与"_binary"的替换
		try {
			SQLStatementParser parser  = new MySqlStatementParser(origSQL);
			sqlStatement = parser.parseStatement();
		} catch (Throwable e) {
			return origSQL;
		}

		if (!(sqlStatement instanceof SQLInsertStatement)) {
			return origSQL;
		}

		SQLInsertStatement statement = (SQLInsertStatement) sqlStatement;
		List<SQLInsertStatement.ValuesClause> valuesClauseList = statement.getValuesList();
		String tabName = statement.getTableName().getSimpleName();
		List<SQLExpr> colExprs = statement.getColumns();

		boolean isRewrite = false;
		for (int i = 0; i < valuesClauseList.size(); i++) {
			SQLInsertStatement.ValuesClause valuesClause = valuesClauseList.get(i);
			List<SQLExpr> values = valuesClause.getValues();
			for (int j = 0; j < values.size(); j++) {
				SQLExpr sqlExpr = values.get(j);
				if (sqlExpr instanceof SQLHexExpr) {
					String colName = ((SQLIdentifierExpr)colExprs.get(j)).getName();
					int colType = getColumnDataTye(con, tabName, colName);
					SQLExpr newSqlExpr =  super.genMethodExpr4Hex(colType, sqlExpr, charset);
					if (newSqlExpr != null) {
						values.set(j, newSqlExpr);
						isRewrite = true;
					}
				} else if (sqlExpr instanceof MySqlCharExpr) {
					super.CleanBinaryCharset( (MySqlCharExpr) sqlExpr);
					isRewrite = true;
				}
			}
		}

		if (isRewrite) {
			return sqlStatement.toString();
		}
		return origSQL;
	}
}
