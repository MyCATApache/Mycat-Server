package io.mycat.route.impl.middlerResultStrategy;

import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelect;

import java.util.List;

/**
 * SQL二元操作中间结果处理
 */
public class BinaryOpResultHandler implements RouteMiddlerReaultHandler {

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
		SQLBinaryOpExpr pp = (SQLBinaryOpExpr)parent;
		if(pp.getLeft() instanceof SQLQueryExpr){
			SQLQueryExpr left = (SQLQueryExpr)pp.getLeft();
			if(left.getSubQuery().equals(sqlselect)){
				SQLExprImpl listExpr = null;
				if(null==param||param.isEmpty()){
					listExpr = new SQLNullExpr();
				}else{
					listExpr = new SQLListExpr();
					listExpr.setParent(left.getParent());
					((SQLListExpr)listExpr).getItems().addAll(param);
				}
				pp.setLeft(listExpr);
			}
		}else if(pp.getRight() instanceof SQLQueryExpr){
			SQLQueryExpr right = (SQLQueryExpr)pp.getRight();
			if(right.getSubQuery().equals(sqlselect)){
				SQLExprImpl listExpr = null;
				if(null==param||param.isEmpty()){
					listExpr = new SQLNullExpr();
				}else{
					listExpr = new SQLListExpr();
					listExpr.setParent(right.getParent());
					((SQLListExpr)listExpr).getItems().addAll(param);
				}
				pp.setRight(listExpr);
				
			}
		}else if(pp.getLeft() instanceof SQLInSubQueryExpr){
			SQLInSubQueryExpr left = (SQLInSubQueryExpr)pp.getLeft();
			if(left.getSubQuery().equals(sqlselect)){
				SQLExprImpl inlistExpr = null;
				if(null==param||param.isEmpty()){
					inlistExpr = new SQLNullExpr();
				}else{
					inlistExpr = new SQLInListExpr();
					((SQLInListExpr)inlistExpr).setTargetList(param);
					((SQLInListExpr)inlistExpr).setExpr(pp.getRight());
					((SQLInListExpr)inlistExpr).setNot(left.isNot());
					((SQLInListExpr)inlistExpr).setParent(left.getParent());
				}
				pp.setLeft(inlistExpr);
			}
		}else if(pp.getRight() instanceof SQLInSubQueryExpr){
			SQLInSubQueryExpr right = (SQLInSubQueryExpr)pp.getRight();
			if(right.getSubQuery().equals(sqlselect)){
				SQLExprImpl listExpr = null;
				if(null==param||param.isEmpty()){
					listExpr = new SQLNullExpr();
				}else{
					listExpr = new SQLListExpr();
					((SQLListExpr)listExpr).getItems().addAll(param);
				}
				pp.setRight(listExpr);
				
			}
		}
		return statement.toString();
	}

}
