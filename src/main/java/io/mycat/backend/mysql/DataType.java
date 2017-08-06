package io.mycat.backend.mysql;

/**
 * 定义返回的数据类型
 * @author huangyiming
 *
 */
public enum  DataType {

	STRING("String"),DOUBLE("Double"),FLOAT("Float"),DATE("Date"),INT("Int");
	private String type;
	private DataType(String type){
		this.type = type;
	}
	public String getType() {
		return type;
	}
	
	
}
