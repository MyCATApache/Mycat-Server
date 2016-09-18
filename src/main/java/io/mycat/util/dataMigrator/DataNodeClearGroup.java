package io.mycat.util.dataMigrator;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据节点按照主机ip进行分组
 * @author haonan108
 *
 */
public class DataNodeClearGroup {

	private String ip;
	private Map<File,DataNode>  tempFiles = new HashMap<>();
	private TableMigrateInfo tableInfo;
	
	public DataNodeClearGroup(String ip, TableMigrateInfo tableInfo) {
		super();
		this.ip = ip;
		this.tableInfo = tableInfo;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public Map<File,DataNode> getTempFiles() {
		return tempFiles;
	}
	public void setTempFiles(Map<File,DataNode> tempFiles) {
		this.tempFiles = tempFiles;
	}
	public TableMigrateInfo getTableInfo() {
		return tableInfo;
	}
	public void setTableInfo(TableMigrateInfo tableInfo) {
		this.tableInfo = tableInfo;
	}
	
}
