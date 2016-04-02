package io.mycat.sqlexecute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TestPrepareSql {

	private static String url = "jdbc:mysql://localhost:8066/test?useServerPrepStmts=true"; // 使用服务端预处理
	private static String user = "zhuam";
	private static String password = "zhuam";
	
	static {
		try {
			// 加载MySql的驱动类
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("找不到驱动程序类 ，加载驱动失败！");
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		testServerPrepareInsertWithBingParam();
	}
	
	/**
	 * 测试服务端预处理批量插入,动态绑定插入参数
	 */
	public static void testServerPrepareInsertWithBingParam() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			String sql = "insert into v1test(id,name1) values(?,?)";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			int startId = 100;
			int batchSize = 10;
			int count = 0;
			while(count < batchSize) {
				pstmt.setInt(1, (int)(System.currentTimeMillis() / 1000L) + startId);
				pstmt.setString(2, "wowo" + startId);
				startId++;
				count++;
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.setAutoCommit(true);
			pstmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}
