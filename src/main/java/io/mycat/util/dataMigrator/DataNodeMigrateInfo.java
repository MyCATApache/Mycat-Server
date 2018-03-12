package io.mycat.util.dataMigrator;

import java.io.File;

/**
 * 数据迁移时数据节点间迁移信息
 * @author haonan108
 *
 */
public class DataNodeMigrateInfo {

	private DataNode src;
	private DataNode target;
	private File tempFile;
	private long size;
	private TableMigrateInfo table;
	
	public DataNodeMigrateInfo(TableMigrateInfo table, DataNode src, DataNode target, File tempFile, long size) {
		super();
		this.table = table;
		this.src = src;
		this.target = target;
		this.tempFile = tempFile;
		this.size = size;
	}
	
	public TableMigrateInfo getTable() {
		return table;
	}

	public void setTable(TableMigrateInfo table) {
		this.table = table;
	}

	public DataNode getSrc() {
		return src;
	}
	public void setSrc(DataNode src) {
		this.src = src;
	}
	public DataNode getTarget() {
		return target;
	}
	public void setTarget(DataNode target) {
		this.target = target;
	}
	public File getTempFile() {
		return tempFile;
	}
	public void setTempFile(File tempFile) {
		this.tempFile = tempFile;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	
}
