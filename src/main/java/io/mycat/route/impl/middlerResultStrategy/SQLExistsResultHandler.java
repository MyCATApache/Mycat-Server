package io.mycat.route.impl.middlerResultStrategy;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.List;

/**
 * SQL Exists中间结果处理
 * 对于 EXISTS/NOT EXISTS, 判断subquery结果集是否为空。
 * @author lyj
 *
 */
public class SQLExistsResultHandler implements RouteMiddlerReaultHandler {

	/**
	 * 处理中间结果
	 * @param statement
	 * @param sqlselect
	 * @param parent
	 * @param param
	 * @return
	 */
	@Override
	public String dohandler(SQLStatement statement, SQLSelect sqlselect, SQLObject parent, List param) {
		SQLExpr se = null;
		
		if(param==null||param.isEmpty()){
			se = new SQLNullExpr();
		}else{
			se = (SQLExpr) param.get(0);
		}
		
		if(parent.getParent() instanceof MySqlSelectQueryBlock){
			MySqlSelectQueryBlock msqb = (MySqlSelectQueryBlock)parent.getParent();
			msqb.setWhere(se);
		}else if(parent.getParent() instanceof SQLBinaryOpExpr){
			SQLBinaryOpExpr sbqe=(SQLBinaryOpExpr)parent.getParent();
			
			if(sbqe.getLeft().equals(parent)){
				sbqe.setLeft(se);
			}else{
				sbqe.setRight(se);
			}
		}
		return statement.toString();
	}

}
