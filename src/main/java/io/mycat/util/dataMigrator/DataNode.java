package io.mycat.util.dataMigrator;

/**
 * 数据节点，精确到库名称
 * @author haonan108
 *
 */
public class DataNode{

	private String name;
	private String ip;
	private int port;
	private String userName;
	private String pwd;
	private String db;
	private String dbType;
	private int index;
	
	public DataNode(String name,String ip, int port, String userName, String pwd, String db,String dbType,int index) {
		super();
		this.name = name;
		this.ip = ip;
		this.port = port;
		this.userName = userName;
		this.pwd = pwd;
		this.db = db;
		this.index = index;
		this.dbType = dbType;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getDbType() {
		return dbType;
	}

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	//暂时只提供mysql驱动
	public String getUrl(){
		return "jdbc:mysql://"+ip+":"+port+"/"+db;
	}
	
	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	@Override
	public String toString(){
		return this.name;
	}

	@Override
	public boolean equals(Object o){
		if(o == null) return false;
		if (this == o) return true;
		
		if(o instanceof DataNode){
			DataNode other = (DataNode)o;
			if (other.getUrl().equals(this.getUrl())){
				return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode(){
		return this.getUrl().hashCode();
	}
	
}
