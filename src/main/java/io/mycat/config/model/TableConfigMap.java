package io.mycat.config.model;

import java.util.HashMap;

/**
 * 支持表名中包含引号[`] 
 * 
 * @author BEN GONG
 */
public class TableConfigMap extends HashMap<String, TableConfig> {

	private static final long serialVersionUID = -6605226933829917213L;

	@Override
	public TableConfig get(Object key) {
		String tableName = key.toString();
		// 忽略表名中的引号。
		if(tableName.contains("`")) {
			tableName = tableName.replaceAll("`", "");
		}
		
		return super.get(tableName);
	}

	@Override
	public boolean containsKey(Object key) {
		String tableName = key.toString();
		// 忽略表名中的引号。
		if(tableName.contains("`")) {
			tableName = tableName.replaceAll("`", "");
		}
		
		return super.containsKey(tableName);
	}
}
