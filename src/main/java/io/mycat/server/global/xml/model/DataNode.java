package io.mycat.server.global.xml.model;

import java.util.List;

public class DataNode {

	private String name;

	private List<Table> tables;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Table> getTables() {
		return tables;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}
	
	public Table getTableByName(String tableName) {
		for(Table table:tables) {
			if(tableName.equals(table.getName())){
				return table;
			}
		}
		return null;
	}
	

}
