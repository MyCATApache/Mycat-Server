package io.mycat.backend.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface SqlRewriter {
	public String rewrite(Connection con, String origSQL, String charset) throws SQLException;
}
