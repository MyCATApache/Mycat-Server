package io.mycat.route.impl.middlerResultStrategy;

import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

/**
 * 对于 EXISTS/NOT EXISTS, 判断subquery结果集是否为空。
 * @author lyj
 *
 */
public class SQLExistsResultHandler implements RouteMiddlerReaultHandler {

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
