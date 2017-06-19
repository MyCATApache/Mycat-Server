package io.mycat.route.impl.middlerResultStrategy;

import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

public class SQLQueryResultHandler implements RouteMiddlerReaultHandler {

	@Override
	public String dohandler(SQLStatement statement, SQLSelect sqlselect, SQLObject parent, List param) {
		if(parent.getParent() instanceof SQLBinaryOpExpr){
			SQLBinaryOpExpr pp = (SQLBinaryOpExpr)parent.getParent();
			SQLExprImpl listExpr = null;
			if(null==param||param.isEmpty()){
				listExpr = new SQLNullExpr();
			}else{
				listExpr = new SQLListExpr();
				((SQLListExpr)listExpr).getItems().addAll(param);
			}
			if(pp.getLeft().equals(parent)){
				pp.setLeft(listExpr);
			}else if(pp.getRight().equals(parent)){
				pp.setRight(listExpr);
			}
		}else if(parent.getParent() instanceof SQLSelectItem){
			SQLSelectItem pp = (SQLSelectItem)parent.getParent();
			SQLExprImpl listExpr = null;
			if(null==param||param.isEmpty()){
				listExpr = new SQLNullExpr();
			}else{
				listExpr = new SQLListExpr();
				((SQLListExpr)listExpr).getItems().addAll(param);
			}
			pp.setExpr(listExpr);
		}else if(parent.getParent() instanceof SQLSelectGroupByClause){
			SQLSelectGroupByClause pp = (SQLSelectGroupByClause)parent.getParent();
			
			List<SQLExpr> items = pp.getItems();
			for(int i=0;i<items.size();i++){
				SQLExpr expr = items.get(i);
				if(expr instanceof SQLQueryExpr 
						&&((SQLQueryExpr)expr).getSubQuery().equals(sqlselect)){
					
					SQLExprImpl listExpr = null;
					if(null==param||param.isEmpty()){
						listExpr = new SQLNullExpr();
					}else{
						listExpr = new SQLListExpr();
						((SQLListExpr)listExpr).getItems().addAll(param);
					}
					items.set(i, listExpr);
				}
			}
		}else if(parent.getParent() instanceof SQLSelectOrderByItem){
			SQLSelectOrderByItem orderItem = (SQLSelectOrderByItem)parent.getParent();
			SQLExprImpl listExpr = null;
			if(null==param||param.isEmpty()){
				listExpr = new SQLNullExpr();
			}else{
				listExpr = new SQLListExpr();
				((SQLListExpr)listExpr).getItems().addAll(param);
			}
			listExpr.setParent(orderItem);
			orderItem.setExpr(listExpr);
		}else if(parent.getParent() instanceof MySqlSelectQueryBlock){
			MySqlSelectQueryBlock query = (MySqlSelectQueryBlock)parent.getParent();
			// select * from subtest1 a where (select 1 from subtest3); 这种情况会进入到当前分支.
			// 改写为   select * from subtest1 a where (1); 或  select * from subtest1 a where (null);
			SQLExprImpl listExpr = null;
			if(null==param||param.isEmpty()){
				listExpr = new SQLNullExpr();
			}else{
				listExpr = new SQLListExpr();
				((SQLListExpr)listExpr).getItems().addAll(param);
			}
			listExpr.setParent(query);
			query.setWhere(listExpr);
		}
		return statement.toString();
	}

}
