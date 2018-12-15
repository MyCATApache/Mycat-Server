package io.mycat.backend.jdbc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class UpdateSqlBinary4PgRewriter extends AbstractSqlBinary4PgRewriter {
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

		if (!(sqlStatement instanceof SQLUpdateStatement)) {
			return origSQL;
		}

		SQLUpdateStatement statement = (SQLUpdateStatement) sqlStatement;
		List<SQLUpdateSetItem> updateSetItems =  statement.getItems();
		String tabName = statement.getTableName().getSimpleName();

		boolean isRewrite = false;
		for (int i = 0 ; i < updateSetItems.size(); i++) {
			SQLUpdateSetItem updateSetItem = updateSetItems.get(i);
			SQLExpr sqlExpr = updateSetItem.getValue();

			if (sqlExpr instanceof SQLHexExpr) {
				String colName = ((SQLIdentifierExpr)updateSetItem.getColumn()).getName();
				int colType = getColumnDataTye(con, tabName, colName);
				SQLExpr newSqlExpr =  super.genMethodExpr4Hex(colType, sqlExpr, charset);
				if (newSqlExpr != null) {
					updateSetItem.setValue(newSqlExpr);
					isRewrite = true;
				}
			} else if (sqlExpr instanceof MySqlCharExpr) {
				super.CleanBinaryCharset((MySqlCharExpr)sqlExpr);
				isRewrite = true;
			} else if (sqlExpr instanceof SQLListExpr) {
				List<SQLExpr> colExprs = ((SQLListExpr)updateSetItem.getColumn()).getItems();
				List<SQLExpr> valExprs = ((SQLListExpr) sqlExpr).getItems();
				for (int j = 0; j < valExprs.size(); j++) {
					SQLExpr sqlExpr2 = valExprs.get(j);
					if (sqlExpr2 instanceof SQLHexExpr) {
						String colName = ((SQLIdentifierExpr)colExprs.get(j)).getName();
						int colType = getColumnDataTye(con, tabName, colName);
						SQLExpr newSqlExpr =  super.genMethodExpr4Hex(colType, sqlExpr2, charset);
						if (newSqlExpr != null) {
							valExprs.set(j, newSqlExpr);
							isRewrite = true;
						}
					} else if (sqlExpr2 instanceof MySqlCharExpr) {
						super.CleanBinaryCharset((MySqlCharExpr)sqlExpr2);
						isRewrite = true;
					}
				}
			}
		}

		if (isRewrite) {
			return sqlStatement.toString();
		}
		return origSQL;
	}
}
