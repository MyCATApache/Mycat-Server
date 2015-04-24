package org.opencloudb.parser.druid;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.stat.TableStat.Condition;

/**
 * Where条件单元
 * 
 * @author wang.dw
 * @date 2015-3-17 下午4:21:21
 * @version 0.1.0 
 * @copyright wonhigh.cn
 */
public class WhereUnit {
	/**
	 * 完整的where条件
	 */
	private SQLBinaryOpExpr whereExpr;
	
	/**
	 * 还能继续再分的表达式:可能还有or关键字
	 */
	private SQLBinaryOpExpr canSplitExpr;
	
	private List<SQLExpr> splitedExprList = new ArrayList<SQLExpr>();
	
	private List<List<Condition>> conditionList = new ArrayList<List<Condition>>();
	
	public WhereUnit(SQLBinaryOpExpr whereExpr) {
		this.whereExpr = whereExpr;
		this.canSplitExpr = whereExpr;
	}

	public SQLBinaryOpExpr getWhereExpr() {
		return whereExpr;
	}

	public void setWhereExpr(SQLBinaryOpExpr whereExpr) {
		this.whereExpr = whereExpr;
	}

	public SQLBinaryOpExpr getCanSplitExpr() {
		return canSplitExpr;
	}

	public void setCanSplitExpr(SQLBinaryOpExpr canSplitExpr) {
		this.canSplitExpr = canSplitExpr;
	}

	public List<SQLExpr> getSplitedExprList() {
		return splitedExprList;
	}

	public void addSplitedExpr(SQLExpr splitedExpr) {
		this.splitedExprList.add(splitedExpr);
	}

	public List<List<Condition>> getConditionList() {
		return conditionList;
	}

	public void setConditionList(List<List<Condition>> conditionList) {
		this.conditionList = conditionList;
	}
}
