package org.opencloudb.sqlexecute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MultiThreadSelectTest {
	private static void testSequnce(Connection theCon) throws SQLException {
		boolean autCommit = System.currentTimeMillis() % 2 == 1;
		theCon.setAutoCommit(autCommit);

		String sql = "select * from company ";
		Statement stmt = theCon.createStatement();
		int charChoise = (int) (System.currentTimeMillis() % 3);
		if (charChoise == 0) {
			stmt.executeQuery("SET NAMES UTF8;");
		} else if (charChoise == 1) {
			stmt.executeQuery("SET NAMES latin1;");
		}
		if (charChoise == 2) {
			stmt.executeQuery("SET NAMES gb2312;");
		}
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			System.out.println(Thread.currentThread().getName() + " get seq " + rs.getLong(1));
		} else {
			System.out.println(Thread.currentThread().getName() + " can't get  seq ");
		}
		if (autCommit == false) {
			theCon.commit();
		}
		stmt.close();

	}

	private static Connection getCon(String url, String user, String passwd) throws SQLException {
		Connection theCon = DriverManager.getConnection(url, user, passwd);
		return theCon;
	}

	public static void main(String[] args) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		}

		final String url = "jdbc:mysql://localhost:8066/TESTDB";
		final String user = "test";
		final String password = "test";
		List<Thread> threads = new ArrayList<Thread>(100);
		for (int i = 0; i < 50; i++) {

			threads.add(new Thread() {
				public void run() {
					Connection con;
					try {
						con = getCon(url, user, password);
						for (int i = 0; i < 10000; i++) {
							testSequnce(con);
						}
					} catch (SQLException e) {

						e.printStackTrace();
					}

				}
			});

		}
		for (Thread thred : threads) {
			thred.start();

		}
		boolean hasRunning = true;
		while (hasRunning) {
			hasRunning = false;
			for (Thread thred : threads) {
				if (thred.isAlive()) {
					try {
						Thread.sleep(1000);
						hasRunning = true;
					} catch (InterruptedException e) {

					}
				}

			}
		}

	}
}
