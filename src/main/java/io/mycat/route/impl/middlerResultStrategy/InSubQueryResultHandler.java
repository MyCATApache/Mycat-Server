package io.mycat.route.impl.middlerResultStrategy;

import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.List;

/**
 * 子查询中间结果处理
 */
public class InSubQueryResultHandler implements RouteMiddlerReaultHandler {

	/**
	 * 处理中间结果
	 * @param statement
	 * @param sqlselect
	 * @param parent
	 * @param param
	 * @return
	 */
	@Override
	public String dohandler(SQLStatement statement,SQLSelect sqlselect,SQLObject parent,List param) {
		SQLExprImpl inlistExpr = null;
		if(null==param||param.isEmpty()){
			inlistExpr = new SQLNullExpr();
		}else{
			inlistExpr = new SQLInListExpr();
			((SQLInListExpr)inlistExpr).setTargetList(param);
			((SQLInListExpr)inlistExpr).setExpr(((SQLInSubQueryExpr)parent).getExpr());
			((SQLInListExpr)inlistExpr).setNot(((SQLInSubQueryExpr)parent).isNot());
			((SQLInListExpr)inlistExpr).setParent(sqlselect.getParent());
		}
		if(parent.getParent() instanceof MySqlSelectQueryBlock){
			((MySqlSelectQueryBlock)parent.getParent()).setWhere(inlistExpr);
		}else if(parent.getParent() instanceof SQLBinaryOpExpr){
			SQLBinaryOpExpr pp = ((SQLBinaryOpExpr)parent.getParent());
			if(pp.getLeft().equals(parent)){
				pp.setLeft(inlistExpr);
			}else if(pp.getRight().equals(parent)){
				pp.setRight(inlistExpr);
			}
		}
		return statement.toString();
	}

}
