package io.mycat.route.impl.middlerResultStrategy;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;

import java.util.List;

/**
 * 路由中间结果处理
 */
public interface RouteMiddlerReaultHandler {
	
	/**
	 * 处理中间结果
	 * @param statement
	 * @param sqlselect
	 * @param param
	 * @return
	 */
	String dohandler(SQLStatement statement, SQLSelect sqlselect, SQLObject parent, List param);

}
