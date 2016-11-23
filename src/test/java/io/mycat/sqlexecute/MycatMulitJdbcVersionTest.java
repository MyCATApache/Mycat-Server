package io.mycat.sqlexecute;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * 
 * 测试mycat对不同版本的mysql jdbc的兼容性 
 * 
 * 
 * <p>
 * 关联issue: @see https://github.com/MyCATApache/Mycat-Server/issues/1203
 * 
 * <p>
 * <b>Note:</b> </br>
 * 1. 请将这个类放到新建的project独立运行, mycat pom.xml里面使用的mysql驱动会影响测试结果 </br>
 * 2. 确保project新建lib子目录并且在lib子目录里面放置了各类版本的mysql jdbc驱动
 * 3. 程序会动态加载不同版本的jdbc驱动, 请不要将任何mysql jdbc驱动加入classpath, 否则也可能影响测试结果
 * 
 * @author CrazyPig
 * @since 2016-11-13
 *
 */
public class MycatMulitJdbcVersionTest {
	
	private static final String JDBC_URL = "jdbc:mysql://localhost:8066/TESTDB";
	private static final String USER = "root";
	private static final String PASSWORD = "123456";
	private static final Map<String, String> jdbcVersionMap = new HashMap<String, String>();
	private static final Map<String, Driver> tmpDriverMap = new HashMap<String, Driver>();
	
	// 动态加载jdbc驱动
	private static void dynamicLoadJdbc(String mysqlJdbcFile) throws Exception {
		URL u = new URL("jar:file:lib/" + mysqlJdbcFile + "!/");
		String classname = jdbcVersionMap.get(mysqlJdbcFile);
		URLClassLoader ucl = new URLClassLoader(new URL[] { u });
		Driver d = (Driver)Class.forName(classname, true, ucl).newInstance();
		DriverShim driver = new DriverShim(d);
		DriverManager.registerDriver(driver);
		tmpDriverMap.put(mysqlJdbcFile, driver);
	}
	
	// 每一次测试完卸载对应版本的jdbc驱动
	private static void dynamicUnLoadJdbc(String mysqlJdbcFile) throws SQLException {
		DriverManager.deregisterDriver(tmpDriverMap.get(mysqlJdbcFile));
	}
	
	// 进行一次测试
	private static void testOneVersion(String mysqlJdbcFile) {
		
		System.out.println("start test mysql jdbc version : " + mysqlJdbcFile);
		
		try {
			dynamicLoadJdbc(mysqlJdbcFile);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(JDBC_URL, USER, PASSWORD);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("select user()");
			System.out.println("select user() output : ");
			while(rs.next()) {
				System.out.println(rs.getObject(1));
			}
			rs = stmt.executeQuery("show tables");
			System.out.println("show tables output : ");
			while(rs.next()) {
				System.out.println(rs.getObject(1));
			}
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
		
		try {
			dynamicUnLoadJdbc(mysqlJdbcFile);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		System.out.println("end !!!");
		System.out.println();
	}
	
	public static void main(String[] args) {
		
		// 多版本mysql jdbc驱动兼容性测试
		
		// NOTE: 注意将对应的jar放到lib子目录, 不需要加入classpath!!!
		jdbcVersionMap.put("mysql-connector-java-6.0.3.jar", "com.mysql.cj.jdbc.Driver");
		jdbcVersionMap.put("mysql-connector-java-5.1.6.jar", "com.mysql.jdbc.Driver");
		jdbcVersionMap.put("mysql-connector-java-5.1.31.jar", "com.mysql.jdbc.Driver");
		jdbcVersionMap.put("mysql-connector-java-5.1.35.jar", "com.mysql.jdbc.Driver");
		jdbcVersionMap.put("mysql-connector-java-5.1.39.jar", "com.mysql.jdbc.Driver");
		
		// 更多的jdbc驱动...
		
		for(String mysqlJdbcFile : jdbcVersionMap.keySet()) {
			testOneVersion(mysqlJdbcFile);
		}
		
	}

}

class DriverShim implements Driver {
    private Driver driver;
    DriverShim(Driver d) { this.driver = d; }
    public boolean acceptsURL(String u) throws SQLException {
        return this.driver.acceptsURL(u);
    }
    public Connection connect(String u, Properties p) throws SQLException {
        return this.driver.connect(u, p);
    }
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return this.driver.getPropertyInfo(url, info);
	}
	@Override
	public int getMajorVersion() {
		return this.driver.getMajorVersion();
	}
	@Override
	public int getMinorVersion() {
		return this.driver.getMinorVersion();
	}
	@Override
	public boolean jdbcCompliant() {
		return this.driver.jdbcCompliant();
	}
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return this.driver.getParentLogger();
	}
}
