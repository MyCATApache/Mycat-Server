package io.mycat.sqlengine;

public class SQLQueryResult<T> {
	private final T result;
	private final boolean success;
	
	private final String dataNode;	// dataNode or database name
	private String tableName;

	public SQLQueryResult(T result, boolean success) {
		super();
		this.result = result;
		this.success = success;
		this.dataNode = null;
	}
	
	public SQLQueryResult(T result, boolean success, String dataNode) {
		super();
		this.result = result;
		this.success = success;
		this.dataNode= dataNode;
	}
	
	public T getResult() {
		return result;
	}
	public boolean isSuccess() {
		return success;
	}
	public String getDataNode() {
		return dataNode;
	}
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
