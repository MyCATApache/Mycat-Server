package org.opencloudb.sqlexecute;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class StandBatchInsertTest {

	public static void testJDBCBatchInsert(Connection theCon)
			throws SQLException {
		theCon.setAutoCommit(false);
		Statement stmt = theCon.createStatement();
		int batchSize = 10;
		for (int i = 0; i < batchSize; i++) {
			String sql = "insert into travelrecord (id,user_id,traveldate,fee,days) values("
					+ i + ",'wang','2014-01-05',510.5,3)";
			stmt.addBatch(sql);
		}
		stmt.executeBatch();
		theCon.commit();
      System.out.println("succees");
	}

	public static void main(String[] args) throws Exception {
		Connection theCon = null;
		try {
			theCon = BaseSQLExeTest.getCon(args);
			testJDBCBatchInsert(theCon);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			BaseSQLExeTest.closeCon(theCon);
		}
	}
}
