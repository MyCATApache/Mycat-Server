package org.opencloudb.stat;

import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;

public class SQLParserHigh {
	public String fixSql(String sql) {
		if ( sql != null)
			return sql.replace("\n", " ");
		return sql;
    }
	
	public String mergeSql(String sql) {
		
		String newSql = ParameterizedOutputVisitorUtils.parameterize(sql, "mysql");
		return fixSql( newSql );
    }

}
