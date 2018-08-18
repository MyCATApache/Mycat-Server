package io.mycat.route.impl.middlerResultStrategy;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;

import java.util.List;

/**
 * SQL ALL中间结果处理
 * 对于 select * from test where id = all (select id from mytab where xxx)
 * 改写后 sql为   all: select * from test where id = val1 and id = val2 …
 *
 * @author lyj
 */
public class SQLAllResultHandler implements RouteMiddlerReaultHandler{

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
		if(parent.getParent() instanceof SQLBinaryOpExpr){
			// sql二元操作表达式
			SQLExprImpl inlistExpr = null;
			if(null==param||param.isEmpty()){
				inlistExpr = new SQLNullExpr();
				SQLBinaryOpExpr xp = (SQLBinaryOpExpr)parent.getParent();
				xp.setOperator(SQLBinaryOperator.Is);
				if(xp.getRight().equals(parent)){
					xp.setRight(inlistExpr);
				}else if(xp.getLeft().equals(parent)){
					xp.setLeft(inlistExpr);
				}
			}else{
				int len = param.size();
				
				SQLBinaryOpExpr xp = (SQLBinaryOpExpr)parent.getParent();
				SQLExpr left = null;
				if(xp.getRight().equals(parent)){
					left = xp.getLeft();
				}else if(xp.getLeft().equals(parent)){
					left = xp.getRight();
				}
								
				SQLBinaryOpExpr p = xp;
				for(int i=0;i<len;i++){
					if(i<(len-1)){
						SQLBinaryOpExpr rightop = new SQLBinaryOpExpr();
						rightop.setOperator(SQLBinaryOperator.Equality);
						SQLValuableExpr expr = (SQLValuableExpr) param.get(i);
						rightop.setRight(expr);
						rightop.setParent(p);
						rightop.setLeft(left);
						p.setRight(rightop);
						p.setOperator(SQLBinaryOperator.BooleanAnd);
						
						SQLBinaryOpExpr lefeop = new SQLBinaryOpExpr();
						lefeop.setParent(p);
						lefeop.setOperator(SQLBinaryOperator.Equality);
						p.setLeft(lefeop);
						p = (SQLBinaryOpExpr) p.getLeft();
					}else{
						p.setLeft(left);
						p.setOperator(SQLBinaryOperator.Equality);
						SQLValuableExpr expr = (SQLValuableExpr) param.get(i);
						p.setRight(expr);
					}
				}
			}
		}
		return statement.toString();
	}

}
