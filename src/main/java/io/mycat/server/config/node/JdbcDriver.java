package io.mycat.server.config.node;

/**
 * 
 * @author Administrator
 */
public class JdbcDriver {
	private String dbType;		// 是哪种数据库的驱动，驱动对应的数据库的种类名称
	private String className;	// 驱动类名
	// 后续可能还要增加其他字段
	
	public JdbcDriver(){}
	
	public JdbcDriver(String dbType, String className){
		this.dbType = dbType;
		this.className = className;
	}
	public String getDbType() {
		return dbType;
	}
	public void setDbType(String dbType) {
		this.dbType = dbType;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
}
