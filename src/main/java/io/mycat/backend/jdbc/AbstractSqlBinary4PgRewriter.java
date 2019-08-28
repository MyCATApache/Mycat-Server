package io.mycat.backend.jdbc;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;

import java.sql.*;

public abstract class AbstractSqlBinary4PgRewriter implements SqlRewriter {
	public abstract String rewrite(Connection con, String origSQL, String charset) throws SQLException;

	protected SQLMethodInvokeExpr genDecodeHex(SQLExpr sqlExpr) {
		SQLMethodInvokeExpr newSqlExpr = new SQLMethodInvokeExpr();
		newSqlExpr.setMethodName("decode");

		SQLCharExpr para = new SQLCharExpr();
		para.setText(((SQLHexExpr) sqlExpr).getHex());
		newSqlExpr.addParameter(para);

		para = new SQLCharExpr();
		para.setText("hex");
		newSqlExpr.addParameter(para);

		return newSqlExpr;
	}


	protected SQLMethodInvokeExpr genConvertFrom(SQLExpr sqlExpr, String charSet) {
		SQLMethodInvokeExpr newSqlExpr = new SQLMethodInvokeExpr();
		newSqlExpr.setMethodName("convert_from");

		newSqlExpr.addParameter(sqlExpr);

		SQLCharExpr para = new SQLCharExpr();
		para.setText(charSet);
		newSqlExpr.addParameter(para);

		return newSqlExpr;
	}

	protected void CleanBinaryCharset(MySqlCharExpr charExpr) {
		if (charExpr.getCharset().compareToIgnoreCase("_binary") == 0 ) {
			charExpr.setCharset(null);
		}
	}

	protected SQLMethodInvokeExpr genMethodExpr4Hex(int colType, SQLExpr sqlExpr, String charset) {
		switch (colType) {
			case Types.ARRAY:
			case Types.BIT:
				return null;
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
			case Types.BLOB:
				return genDecodeHex(sqlExpr);
			default:
				return genConvertFrom(genDecodeHex(sqlExpr), charset);
		}
	}
	protected  int getColumnDataTye(Connection con, String tabName, String colName) throws SQLException {
		int dataType = Types.BINARY;
		DatabaseMetaData metaData = con.getMetaData();
		ResultSet rs = metaData.getColumns(null, null, tabName.toLowerCase(), colName.toLowerCase());
		while (rs.next()) {
			dataType = rs.getInt("DATA_TYPE");
		}
		rs.close();
		return dataType;
	}
}
