package io.mycat.sqlexecute;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TestPrepareSql {

	private static String url = "jdbc:mysql://localhost:8066/TESTDB?useServerPrepStmts=true"; // 使用服务端预处理
//	private static String url = "jdbc:mysql://localhost:8066/TESTDB";
	private static String user = "test";
	private static String password = "test";
	
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
//		testServerPrepareSelectNormal();
//		testServerPrepareSelectWithBingingParam();
//		testServerPrepareInsertWithBingParam();
//		testServerPrepareSelectWithNumericType();
		testServerPrepareSelectWithDateType();
//		testServerPrepareSelectWithStringType();
	}
	
	/**
	 * 测试服务端预处理批量插入,动态绑定插入参数
	 */
	public static void testServerPrepareInsertWithBingParam() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			String sql = "insert into company(id,name) values(?,?)";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			int startId = 100;
			int batchSize = 10;
			int count = 0;
			while(count < batchSize) {
				pstmt.setInt(1, startId);
				pstmt.setString(2, "测试公司" + startId);
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
	
	/**
	 * 测试服务端预处理查询,动态绑定查询参数
	 */
	public static void testServerPrepareSelectWithBingingParam() {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
			String sql = "select * from company where id > ?";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setInt(1, 1);
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columns = rsmd.getColumnCount();
			for(int i = 1; i <= columns; i++) { // 输出列名
				System.out.print(rsmd.getColumnName(i) + "\t");
			}
			System.out.println();
			while(rs.next()) {
				for(int i = 1; i <= columns; i++) { // 输出行
					System.out.print(rs.getObject(i) + "\t");
				}
				System.out.println();
			}
			rs.close();
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
	
	
//	mysql> desc test_numeric;
//	+---------------+---------------+------+-----+---------+----------------+
//	| Field         | Type          | Null | Key | Default | Extra          |
//	+---------------+---------------+------+-----+---------+----------------+
//	| id            | int(11)       | NO   | PRI | NULL    | auto_increment |
//	| tinyint_val   | tinyint(4)    | YES  |     | NULL    |                |
//	| smallint_val  | smallint(6)   | YES  |     | NULL    |                |
//	| mediumint_val | decimal(11,0) | YES  |     | NULL    |                |
//	| int_val       | int(11)       | YES  |     | NULL    |                |
//	| bigint_val    | bigint(20)    | YES  |     | NULL    |                |
//	| decimal_val   | decimal(7,2)  | YES  |     | NULL    |                |
//	| float_val     | float(7,2)    | YES  |     | NULL    |                |
//	| double_val    | double(7,2)   | YES  |     | NULL    |                |
//	+---------------+---------------+------+-----+---------+----------------+
//	9 rows in set (0.00 sec)
	
	/**
	 * 测试服务端预处理查询返回Numeric类型数据是否有误
	 */
	public static void testServerPrepareSelectWithNumericType() {
		String sql = "select * from test_numeric";
		
//		mysql> select * from test_numeric;
//		+----+-------------+--------------+---------------+-----------+------------+-------------+-----------+------------+
//		| id | tinyint_val | smallint_val | mediumint_val | int_val   | bigint_val | decimal_val | float_val | double_val |
//		+----+-------------+--------------+---------------+-----------+------------+-------------+-----------+------------+
//		|  1 |        NULL |         NULL |          NULL |      NULL |       NULL |        NULL |      NULL |       NULL |
//		|  2 |         123 |        12345 |     123456789 | 123456789 |  123456789 |     1234.33 |   1234.33 |    1234.33 |
//		+----+-------------+--------------+---------------+-----------+------------+-------------+-----------+------------+
//		2 rows in set (0.00 sec)
		
		testServerPrepareSelectSql(sql);
	}
	
	
//	mysql> desc test_date;
//	+---------------+-----------+------+-----+-------------------+-----------------------------+
//	| Field         | Type      | Null | Key | Default           | Extra                       |
//	+---------------+-----------+------+-----+-------------------+-----------------------------+
//	| id            | int(11)   | NO   | PRI | NULL              | auto_increment              |
//	| date_val      | date      | YES  |     | NULL              |                             |
//	| datetime_val  | datetime  | YES  |     | NULL              |                             |
//	| timestamp_val | timestamp | NO   |     | CURRENT_TIMESTAMP | on update CURRENT_TIMESTAMP |
//	+---------------+-----------+------+-----+-------------------+-----------------------------+
//	4 rows in set (0.01 sec)
	
	/**
	 * 测试服务端预处理查询返回Date和Time类型数据是否有误
	 */
	public static void testServerPrepareSelectWithDateType() {
		String sql = "select * from test_date";
		
//		mysql> select * from test_date;
//		+----+------------+---------------------+---------------------+
//		| id | date_val   | datetime_val        | timestamp_val       |
//		+----+------------+---------------------+---------------------+
//		|  1 | 2015-08-19 | 2015-08-26 16:02:11 | 2015-08-19 16:02:22 |
//		|  2 | NULL       | NULL                | 2015-08-30 16:02:41 |
//		+----+------------+---------------------+---------------------+
//		2 rows in set (0.00 sec)
		
		testServerPrepareSelectSql(sql);
	}
	
//	mysql> desc test_string;
//	+-------------+-------------+------+-----+---------+----------------+
//	| Field       | Type        | Null | Key | Default | Extra          |
//	+-------------+-------------+------+-----+---------+----------------+
//	| id          | int(11)     | NO   | PRI | NULL    | auto_increment |
//	| char_val    | char(10)    | YES  |     | NULL    |                |
//	| varchar_val | varchar(10) | YES  |     | NULL    |                |
//	| text_val    | text        | YES  |     | NULL    |                |
//	+-------------+-------------+------+-----+---------+----------------+
//	4 rows in set (0.01 sec)
	
	/**
	 * 测试服务端预处理查询返回String类型数据是否有误
	 */
	public static void testServerPrepareSelectWithStringType() {
		String sql = "select * from test_string";
		
//		mysql> select * from test_string;
//		+----+----------+-------------+----------+
//		| id | char_val | varchar_val | text_val |
//		+----+----------+-------------+----------+
//		|  1 | AAA      | BBB         | CCC      |
//		|  2 | NULL     | NULL        | NULL     |
//		|  3 |          |             |          |
//		+----+----------+-------------+----------+
//		3 rows in set (0.00 sec)
		
		testServerPrepareSelectSql(sql);
	}
	
	private static void testServerPrepareSelectSql(String sql) {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columns = rsmd.getColumnCount();
			for(int i = 1; i <= columns; i++) { // 输出列名
				System.out.print(rsmd.getColumnName(i) + "\t");
			}
			System.out.println();
			while(rs.next()) {
				for(int i = 1; i <= columns; i++) { // 输出行
					System.out.print(rs.getObject(i) + "\t");
				}
				System.out.println();
			}
			rs.close();
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
