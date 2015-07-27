package io.mycat.route.parser.druid;

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
 * 
 * 示例：
SELECT id,traveldate
FROM   travelrecord
WHERE  id = 1
       AND ( fee > 0
              OR days > 0
              OR ( traveldate > '2015-05-04 00:00:07.375'
                   AND ( user_id <= 2
                          OR fee = days
                          OR fee > 0 ) ) )
       AND name = 'zhangsan'
ORDER  BY traveldate DESC
LIMIT  20 
 * 
 * 
 * 
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
	
	/**
	 * whereExpr并不是一个where的全部，有部分条件在outConditions
	 */
	private List<Condition> outConditions = new ArrayList<Condition>();

	/**
	 * 按照or拆分后的条件片段中可能还有or语句，这样的片段实际上是嵌套的or语句，将其作为内层子whereUnit，不管嵌套多少层，循环处理
	 */
	private List<WhereUnit> subWhereUnits = new ArrayList<WhereUnit>();
	
	private boolean finishedParse = false;
	
	public List<Condition> getOutConditions() {
		return outConditions;
	}

	public void addOutConditions(List<Condition> outConditions) {
		this.outConditions.addAll(outConditions);
	}
	
	public boolean isFinishedParse() {
		return finishedParse;
	}

	public void setFinishedParse(boolean finishedParse) {
		this.finishedParse = finishedParse;
	}

	public WhereUnit() {
	}
	
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
	
	public void addSubWhereUnit(WhereUnit whereUnit) {
		this.subWhereUnits.add(whereUnit);
	}
	
	public List<WhereUnit> getSubWhereUnit() {
		return this.subWhereUnits;
	}
}
