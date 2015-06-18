package io.mycat.sqlexecute;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TestJdbc {
	public static void main(String[] args) throws SQLException {
		try {
			// 加载MySql的驱动类
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("找不到驱动程序类 ，加载驱动失败！");
			e.printStackTrace();
		}

		// 连接MySql数据库，用户名和密码都是root
		// String url = "jdbc:mysql://192.168.56.101:3309/mycat";
		// String username = "root";
		// String password = "root123";
		String url = "jdbc:mysql://127.0.0.1:8066/TESTDB";
		String username = "test";
		String password = "test";
		Connection con = null;
		try {
			con = DriverManager.getConnection(url, username, password);
		} catch (SQLException se) {
			System.out.println("数据库连接失败！");
			se.printStackTrace();
		}
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			// String sql =
			// "insert into customer (id,name,company_id,sharding_id) values (10001,'test',1,10000)";
			// PreparedStatement pstmt = con.prepareStatement(sql) ;
			// ResultSet rs = stmt.executeQuery("SELECT * FROM ...") ;
			// int rows = stmt.executeUpdate("INSERT INTO ...") ;
			// boolean flag = stmt.execute(String sql)
			// while(rs.next()){
			// String name = rs.getString("name") ;
			// String pass = rs.getString(1) ; // 此方法比较高效
			// }

			con.setAutoCommit(false);
//			stmt = con
//					.prepareStatement("insert into hotnews(id, title,created_time) values(?, ?,?)");
			stmt = con
			.prepareStatement("update company set name=concat(name,'1')");
			// con.setAutoCommit(false);
			// stmt =
			// con.prepareStatement("select * from tb_cm_cust limit 100000");
			long begin = System.currentTimeMillis();
			// con.createStatement().execute("truncate table hotnews");
			System.out.println();
			// for (int loop = 0; loop < 10; loop++) {
			//stmt.setInt(1, 1);
			//stmt.setString(2, 1 + "" + new Date());

			// stmt.setInt(1, loop);
			// stmt.setString(2, loop + "" + new Date());
			//stmt.setDate(3, new java.sql.Date(System.currentTimeMillis()));
			stmt.addBatch();
			// if (loop % 5 == 0) {
			// System.out.println(loop);
			// }
			// }
			//
			stmt.executeBatch();
//			if (true) {
//				throw new RuntimeException("test");
//			}
			con.commit();
			// stmt.executeQuery();
			System.out.println((System.currentTimeMillis() - begin) / 1000.0);
		} catch (Exception e) {
			e.printStackTrace();
			try {
				con.rollback();
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		} finally {

			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
			if (con != null) {
				con.close();
			}
		}
	}
}
