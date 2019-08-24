package io.mycat.config.model;

import java.util.HashMap;
import java.util.Map;


/**
 * 用户 SQL 权限配置
 *
 * @author zhuam
 *
 */
public class UserPrivilegesConfig {
	
	private boolean check = false;

	/** 库级权限 */
	private Map<String, SchemaPrivilege> schemaPrivileges = new HashMap<String, SchemaPrivilege>();

	/** dataNode权限 */
	private Map<String, DataNodePrivilege> dataNodePrivileges = new HashMap<String, DataNodePrivilege>();

	public boolean isCheck() {
		return check;
	}

	public void setCheck(boolean check) {
		this.check = check;
	}

	public void addSchemaPrivilege(String schemaName, SchemaPrivilege privilege) {
		this.schemaPrivileges.put(schemaName, privilege);
	}
	
	public SchemaPrivilege getSchemaPrivilege(String schemaName) {
		return schemaPrivileges.get( schemaName );
	}

	public void addDataNodePrivileges(String dataNodeName, DataNodePrivilege privilege) {
		this.dataNodePrivileges.put(dataNodeName, privilege);
	}

	public DataNodePrivilege getDataNodePrivilege(String dataNodeName) {
		return dataNodePrivileges.get(dataNodeName);
	}

	/**
	 * 库级权限
	 */
	public static class SchemaPrivilege {
		
		private String name;
		private int[] dml = new int[]{0, 0, 0, 0};
		
		private Map<String, TablePrivilege> tablePrivileges = new HashMap<String, TablePrivilege>();
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		public int[] getDml() {
			return dml;
		}
		
		public void setDml(int[] dml) {
			this.dml = dml;
		}
		
		public void addTablePrivilege(String tableName, TablePrivilege privilege) {
			this.tablePrivileges.put(tableName, privilege);
		}
		
		public TablePrivilege getTablePrivilege(String tableName) {
			TablePrivilege tablePrivilege = tablePrivileges.get( tableName );
			if ( tablePrivilege == null ) {
				tablePrivilege = new TablePrivilege();
				tablePrivilege.setName(tableName);
				tablePrivilege.setDml(dml);
			}
			return tablePrivilege;
		}
	}
	
	/**
	 * 表级权限
	 */
	public static class TablePrivilege {

		private String name;
		private int[] dml = new int[] { 0, 0, 0, 0 };

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int[] getDml() {
			return dml;
		}

		public void setDml(int[] dml) {
			this.dml = dml;
		}

	}

	/**
	 * dataNode权限
	 */
	public static class DataNodePrivilege {

		private String name;
		private int[] dml = new int[] { 0, 0, 0, 0 };

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int[] getDml() {
			return dml;
		}

		public void setDml(int[] dml) {
			this.dml = dml;
		}
	}
}