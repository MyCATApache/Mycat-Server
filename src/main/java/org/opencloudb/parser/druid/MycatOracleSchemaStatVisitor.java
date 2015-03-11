package org.opencloudb.parser.druid;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.dialect.oracle.ast.stmt.OracleSelectQueryBlock;
import com.alibaba.druid.sql.dialect.oracle.visitor.OracleSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Condition;

import java.util.Map;

/**
 * Druid解析器中用来从ast语法中提取表名、条件、字段等的vistor
 * @author wang.dw
 *
 */
public class MycatOracleSchemaStatVisitor extends OracleSchemaStatVisitor
{
	@Override
	public boolean visit(SQLSelectStatement x) {
        setAliasMap();
        getAliasMap().put("DUAL", null);

        return true;
    }
	
	@Override
	public boolean visit(SQLBetweenExpr x) {
    	String begin = x.beginExpr.toString();
    	String end = x.endExpr.toString();
    	
    	Column column = getColumn(x);
        if (column == null) {
            return true;
        }

        Condition condition = null;
        for (Condition item : this.getConditions()) {
            if (item.getColumn().equals(column) && item.getOperator().equals("between")) {
                condition = item;
                break;
            }
        }

        if (condition == null) {
            condition = new Condition();
            condition.setColumn(column);
            condition.setOperator("between");
            this.conditions.add(condition);
        }

      
        condition.getValues().add(begin);
        condition.getValues().add(end);

    	
    	return true;
    }
	
	@Override
	protected Column getColumn(SQLExpr expr) {
        Map<String, String> aliasMap = getAliasMap();
        if (aliasMap == null) {
            return null;
        }

        if (expr instanceof SQLPropertyExpr) {
            SQLExpr owner = ((SQLPropertyExpr) expr).getOwner();
            String column = ((SQLPropertyExpr) expr).getName();

            if (owner instanceof SQLIdentifierExpr) {
                String tableName = ((SQLIdentifierExpr) owner).getName();
                String table = tableName;
                if (aliasMap.containsKey(table)) {
                    table = aliasMap.get(table);
                }

                if (variants.containsKey(table)) {
                    return null;
                }

                if (table != null) {
                    return new Column(table, column);
                }

                return handleSubQueryColumn(tableName, column);
            }

            return null;
        }

        if (expr instanceof SQLIdentifierExpr) {
            Column attrColumn = (Column) expr.getAttribute(ATTR_COLUMN);
            if (attrColumn != null) {
                return attrColumn;
            }

            String column = ((SQLIdentifierExpr) expr).getName();
            String table = getCurrentTable();
            if (table != null && aliasMap.containsKey(table)) {
                table = aliasMap.get(table);
                if (table == null) {
                    return null;
                }
            }

            if (table != null) {
                return new Column(table, column);
            }

            if (variants.containsKey(column)) {
                return null;
            }

            return new Column("UNKNOWN", column);
        }
        
        if(expr instanceof SQLBetweenExpr) {
        	SQLBetweenExpr betweenExpr = (SQLBetweenExpr)expr;
        	
        	if(betweenExpr.getTestExpr() != null) {
        		String tableName = null;
        		String column = null;
        		if(betweenExpr.getTestExpr() instanceof SQLPropertyExpr) {//字段带别名的
        			tableName = ((SQLIdentifierExpr)((SQLPropertyExpr) betweenExpr.getTestExpr()).getOwner()).getName();
                    column = ((SQLPropertyExpr) betweenExpr.getTestExpr()).getName();
                    return handleSubQueryColumn(tableName, column);
        		} else if(betweenExpr.getTestExpr() instanceof SQLIdentifierExpr) {
                    column = ((SQLIdentifierExpr) betweenExpr.getTestExpr()).getName();
                  //字段不带别名的,此处如果是多表，容易出现ambiguous，
        			//不知道这个字段是属于哪个表的,fdbparser用了defaultTable，即join语句的leftTable
                    tableName = getOwnerTableName(betweenExpr,column);
        		}
        		String table = tableName;
                if (aliasMap.containsKey(table)) {
                    table = aliasMap.get(table);
                }

                if (variants.containsKey(table)) {
                    return null;
                }

                if (table != null) {
                    return new Column(table, column);
                }
        	}
        	
        	
        }
        return null;
    }
	
	/**
	 * 从between语句中获取字段所属的表名。
	 * 对于容易出现ambiguous的（字段不知道到底属于哪个表），实际应用中必须使用别名来避免歧义
	 * @param betweenExpr
	 * @param column
	 * @return
	 */
	private String getOwnerTableName(SQLBetweenExpr betweenExpr,String column) {
		if(tableStats.size() == 1) {//只有一个表，直接返回这一个表名
			return tableStats.keySet().iterator().next().getName();
		} else if(tableStats.size() == 0) {//一个表都没有，返回空串
			return "";
		} else {//多个表名
			for(Column col : columns) {//从columns中找表名
				if(col.getName().equals(column)) {
					return col.getTable();
				}
			}
			
			//前面没找到表名的，自己从parent中解析
			if(betweenExpr.getParent() instanceof OracleSelectQueryBlock) {
                OracleSelectQueryBlock select = (OracleSelectQueryBlock)betweenExpr.getParent();
				if(select.getFrom() instanceof SQLJoinTableSource) {//多表连接
					SQLJoinTableSource joinTableSource = (SQLJoinTableSource)select.getFrom();
					return joinTableSource.getLeft().toString();//将left作为主表，此处有不严谨处，但也是实在没有办法，如果要准确，字段前带表名或者表的别名即可
				} else if(select.getFrom() instanceof SQLExprTableSource) {//单表
					return select.getFrom().toString();
				}
			} else if(betweenExpr.getParent() instanceof SQLUpdateStatement) {
				SQLUpdateStatement update = (SQLUpdateStatement)betweenExpr.getParent();
				return update.getTableName().getSimpleName();
			} else if(betweenExpr.getParent() instanceof SQLDeleteStatement) {
				SQLDeleteStatement delete = (SQLDeleteStatement)betweenExpr.getParent();
				return delete.getTableName().getSimpleName();
			} else {
				//TODO 带where的其他语句，暂时不考虑
			}
		}
		return "";
	}
	
	@Override
	public boolean visit(SQLBinaryOpExpr x) {
        x.getLeft().setParent(x);
        x.getRight().setParent(x);

        switch (x.getOperator()) {
            case Equality:
            case LessThanOrEqualOrGreaterThan:
            case Is:
            case IsNot:
                handleCondition(x.getLeft(), x.getOperator().name, x.getRight());
                handleCondition(x.getRight(), x.getOperator().name, x.getLeft());
                handleRelationship(x.getLeft(), x.getOperator().name, x.getRight());
                break;
            case Like:
            case NotLike:
            case NotEqual:
            case GreaterThan:
            case GreaterThanOrEqual:
            case LessThan:
            case LessThanOrEqual:
            default:
                break;
        }
        return true;
    }
}
