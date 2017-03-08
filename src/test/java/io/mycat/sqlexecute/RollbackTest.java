package io.mycat.sqlexecute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RollbackTest {
	private static Connection getCon(String url, String user, String passwd)
			throws SQLException {
		return DriverManager.getConnection(url, user, passwd);
	}
	public static void main(String[] args) {
		

	}

}
