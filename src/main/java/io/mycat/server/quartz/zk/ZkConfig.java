/** 
 * @Title:zk配置类  
 * @Desription:zk配置类 
 * @Company:CSN
 * @ClassName:ZkConfig.java
 * @Author:Justic
 * @CreateDate:2015-6-30   
 * @UpdateUser:Justic  
 * @Version:0.1 
 *    
 */

package io.mycat.server.quartz.zk;

/**
 * @ClassName: ZkConfig
 * @Description: zk配置类
 * @author: Justic
 * @date: 2015-6-30
 * 
 */
public class ZkConfig {

	private String zkServer;// ip1:port1,ip2:port2
	private String rootPath;// 节点path
	private String groupName;// 功能相同job归为同一群组
	private int connectTimeout;// 连接zk超时时间
	private int sessionTimeout;// zk-session超时时间
	private String nodeName;// 当前节点名称
	private int RetryNTimes=1000;
	private int connectionTimeoutMs=6000;

	public ZkConfig() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ZkConfig(String zkServer, String rootPath, String nodeName, int connectTimeout,int sessionTimeout) {
		super();
		this.zkServer = zkServer;
		this.rootPath = rootPath;
		this.connectTimeout = connectTimeout;
		this.sessionTimeout = sessionTimeout;
		this.nodeName = nodeName;
	}

	public String getZkServer() {
		return zkServer;
	}

	public void setZkServer(String zkServer) {
		this.zkServer = zkServer;
	}

	public String getRootPath() {
		return rootPath;
	}

	public void setRootPath(String rootPath) {
		this.rootPath = rootPath;
	}

	public String getGroupName() {
		return groupName;
	}

	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public int getSessionTimeout() {
		return sessionTimeout;
	}

	public void setSessionTimeout(int sessionTimeout) {
		this.sessionTimeout = sessionTimeout;
	}

	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}

	public int getRetryNTimes() {
		return RetryNTimes;
	}

	public void setRetryNTimes(int retryNTimes) {
		RetryNTimes = retryNTimes;
	}

	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}

	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

}
