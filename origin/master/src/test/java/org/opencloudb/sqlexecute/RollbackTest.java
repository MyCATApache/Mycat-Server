package org.opencloudb.sqlexecute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RollbackTest {
	private static Connection getCon(String url, String user, String passwd)
			throws SQLException {
		Connection theCon = DriverManager.getConnection(url, user, passwd);
		return theCon;
	}
	public static void main(String[] args) {
		

	}

}
