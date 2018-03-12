package io.mycat.route.parser.druid;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLExprImpl;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllExpr;
import com.alibaba.druid.sql.ast.expr.SQLAnyExpr;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLExistsExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLSomeExpr;
import com.alibaba.druid.sql.ast.expr.SQLValuableExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.druid.stat.TableStat.Mode;
import com.alibaba.druid.stat.TableStat.Relationship;

import io.mycat.route.util.RouterUtil;

/**
 * Druid解析器中用来从ast语法中提取表名、条件、字段等的vistor
 * @author wang.dw
 *
 */
public class MycatSchemaStatVisitor extends MySqlSchemaStatVisitor {
	private boolean hasOrCondition = false;
	private List<WhereUnit> whereUnits = new CopyOnWriteArrayList<WhereUnit>();
	private List<WhereUnit> storedwhereUnits = new CopyOnWriteArrayList<WhereUnit>();
    private Queue<SQLSelect> subQuerys = new LinkedBlockingQueue<>();  //子查询集合
	private boolean hasChange = false; // 是否有改写sql
	private boolean subqueryRelationOr = false;   //子查询存在关联条件的情况下，是否有 or 条件
	
	private void reset() {
		this.conditions.clear();
		this.whereUnits.clear();
		this.hasOrCondition = false;
	}
	
	public List<WhereUnit> getWhereUnits() {
		return whereUnits;
	}

	public boolean hasOrCondition() {
		return hasOrCondition;
	}
	
    @Override
    public boolean visit(SQLSelectStatement x) {
        setAliasMap();
//        getAliasMap().put("DUAL", null);

        return true;
    }

    @Override
    public boolean visit(SQLBetweenExpr x) {
        String begin = null;
        if(x.beginExpr instanceof SQLCharExpr)
        {
            begin= (String) ( (SQLCharExpr)x.beginExpr).getValue();
        }  else {
            begin = x.beginExpr.toString();
        }
        String end = null;
        if(x.endExpr instanceof SQLCharExpr)
        {
            end= (String) ( (SQLCharExpr)x.endExpr).getValue();
        }  else {
            end = x.endExpr.toString();
        }
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
					SQLObject query = this.subQueryMap.get(tableName);
					if(query == null) {
						if (aliasMap.containsKey(tableName)) {
							tableName = aliasMap.get(tableName);
						}
						return new Column(tableName, column);
					}
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

                if (table != null&&!"".equals(table)) {
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
            for (Column col : columns.keySet())
            {
                if(col.getName().equals(column)) {
                    return col.getTable();
                }
            }
//            for(Column col : columns) {//从columns中找表名
//                if(col.getName().equals(column)) {
//                    return col.getTable();
//                }
//            }

            //前面没找到表名的，自己从parent中解析

            SQLObject parent = betweenExpr.getParent();
            if(parent instanceof SQLBinaryOpExpr)
            {
                parent=parent.getParent();
            }

            if(parent instanceof MySqlSelectQueryBlock) {
                MySqlSelectQueryBlock select = (MySqlSelectQueryBlock) parent;
                if(select.getFrom() instanceof SQLJoinTableSource) {//多表连接
                    SQLJoinTableSource joinTableSource = (SQLJoinTableSource)select.getFrom();
                    return joinTableSource.getLeft().toString();//将left作为主表，此处有不严谨处，但也是实在没有办法，如果要准确，字段前带表名或者表的别名即可
                } else if(select.getFrom() instanceof SQLExprTableSource) {//单表
                    return select.getFrom().toString();
                }
            }
            else if(parent instanceof SQLUpdateStatement) {
                SQLUpdateStatement update = (SQLUpdateStatement) parent;
                return update.getTableName().getSimpleName();
            } else if(parent instanceof SQLDeleteStatement) {
                SQLDeleteStatement delete = (SQLDeleteStatement) parent;
                return delete.getTableName().getSimpleName();
            } else {
                
            }
        }
        return "";
    }
    
    private void setSubQueryRelationOrFlag(SQLExprImpl x){
    	MycatSubQueryVisitor subQueryVisitor = new MycatSubQueryVisitor();
    	x.accept(subQueryVisitor);
    	if(subQueryVisitor.isRelationOr()){
    		subqueryRelationOr = true;
    	}
    }
    
    /*
     * 子查询
     * (non-Javadoc)
     * @see com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter#visit(com.alibaba.druid.sql.ast.expr.SQLQueryExpr)
     */
    @Override
    public boolean visit(SQLQueryExpr x) {
    	setSubQueryRelationOrFlag(x);
    	addSubQuerys(x.getSubQuery());
    	return super.visit(x);
    }
    /*
     * (non-Javadoc)
     * @see com.alibaba.druid.sql.visitor.SchemaStatVisitor#visit(com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource)
     */
    @Override
    public boolean visit(SQLSubqueryTableSource x){
    	addSubQuerys(x.getSelect());
    	return super.visit(x);
    }
    
    /*
     * (non-Javadoc)
     * @see com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter#visit(com.alibaba.druid.sql.ast.expr.SQLExistsExpr)
     */
    @Override
    public boolean visit(SQLExistsExpr x) {
    	setSubQueryRelationOrFlag(x);
    	addSubQuerys(x.getSubQuery());
    	return super.visit(x);
    }
    
    @Override
    public boolean visit(SQLInListExpr x) {
    	return super.visit(x);
    }
    
    /*
     *  对 in 子查询的处理
     * (non-Javadoc)
     * @see com.alibaba.druid.sql.visitor.SchemaStatVisitor#visit(com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr)
     */
    @Override
    public boolean visit(SQLInSubQueryExpr x) {
    	setSubQueryRelationOrFlag(x);
    	addSubQuerys(x.getSubQuery());
    	return super.visit(x);
    }
    
    /* 
     *  遇到 all 将子查询改写成  SELECT MAX(name) FROM subtest1
     *  例如:
     *        select * from subtest where id > all (select name from subtest1);
     *    		>/>= all ----> >/>= max
     *    		</<= all ----> </<= min
     *    		<>   all ----> not in
     *          =    all ----> id = 1 and id = 2
     *          other  不改写
     */    
    @Override
    public boolean visit(SQLAllExpr x) {
    	setSubQueryRelationOrFlag(x);
    	
    	List<SQLSelectItem> itemlist = ((SQLSelectQueryBlock)(x.getSubQuery().getQuery())).getSelectList();
    	SQLExpr sexpr = itemlist.get(0).getExpr();
    	
		if(x.getParent() instanceof SQLBinaryOpExpr){
			SQLBinaryOpExpr parentExpr = (SQLBinaryOpExpr)x.getParent();
			SQLAggregateExpr saexpr = null;
			switch (parentExpr.getOperator()) {
			case GreaterThan:
			case GreaterThanOrEqual:
			case NotLessThan:
				this.hasChange = true;
				if(sexpr instanceof SQLIdentifierExpr 
						|| (sexpr instanceof SQLPropertyExpr&&((SQLPropertyExpr)sexpr).getOwner() instanceof SQLIdentifierExpr)){
					saexpr = new SQLAggregateExpr("MAX");
					saexpr.getArguments().add(sexpr);
	        		saexpr.setParent(itemlist.get(0));
	        		itemlist.get(0).setExpr(saexpr);
				}
				SQLQueryExpr maxSubQuery = new SQLQueryExpr(x.getSubQuery());
        		x.getSubQuery().setParent(x.getParent());
        		// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
            	if(x.getParent() instanceof SQLBinaryOpExpr){
            		if(((SQLBinaryOpExpr)x.getParent()).getLeft().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setLeft(maxSubQuery);
            		}else if(((SQLBinaryOpExpr)x.getParent()).getRight().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setRight(maxSubQuery);
            		}
            	}
            	addSubQuerys(x.getSubQuery());
            	return super.visit(x.getSubQuery());
			case LessThan:
			case LessThanOrEqual:
			case NotGreaterThan:
				this.hasChange = true;
				if(sexpr instanceof SQLIdentifierExpr 
						|| (sexpr instanceof SQLPropertyExpr&&((SQLPropertyExpr)sexpr).getOwner() instanceof SQLIdentifierExpr)){
					saexpr = new SQLAggregateExpr("MIN");
					saexpr.getArguments().add(sexpr);
	        		saexpr.setParent(itemlist.get(0));
	        		itemlist.get(0).setExpr(saexpr);
	        		
	            	x.subQuery.setParent(x.getParent());
				}
				// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
            	SQLQueryExpr minSubQuery = new SQLQueryExpr(x.getSubQuery());
            	if(x.getParent() instanceof SQLBinaryOpExpr){
            		if(((SQLBinaryOpExpr)x.getParent()).getLeft().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setLeft(minSubQuery);
            		}else if(((SQLBinaryOpExpr)x.getParent()).getRight().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setRight(minSubQuery);
            		}
            	}
            	addSubQuerys(x.getSubQuery());
            	return super.visit(x.getSubQuery());
			 case LessThanOrGreater:
			 case NotEqual:
				this.hasChange = true;
				SQLInSubQueryExpr notInSubQueryExpr = new SQLInSubQueryExpr(x.getSubQuery());
				x.getSubQuery().setParent(notInSubQueryExpr);
				notInSubQueryExpr.setNot(true);
				// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
				if(x.getParent() instanceof SQLBinaryOpExpr){
					SQLBinaryOpExpr xp = (SQLBinaryOpExpr)x.getParent();
					
					if(xp.getLeft().equals(x)){
						notInSubQueryExpr.setExpr(xp.getRight());
					}else if(xp.getRight().equals(x)){
						notInSubQueryExpr.setExpr(xp.getLeft());
					}
					
					if(xp.getParent() instanceof MySqlSelectQueryBlock){
						((MySqlSelectQueryBlock)xp.getParent()).setWhere(notInSubQueryExpr);
					}else if(xp.getParent() instanceof SQLBinaryOpExpr){
						SQLBinaryOpExpr pp = ((SQLBinaryOpExpr)xp.getParent());
						if(pp.getLeft().equals(xp)){
							pp.setLeft(notInSubQueryExpr);
						}else if(pp.getRight().equals(xp)){
							pp.setRight(notInSubQueryExpr);
						}
					}
	            }
				addSubQuerys(x.getSubQuery());
	            return super.visit(notInSubQueryExpr);
			 default:
				break;
			}
		}
		addSubQuerys(x.getSubQuery());
    	return super.visit(x);
    }
    
    /* 
     *  遇到 some 将子查询改写成  SELECT MIN(name) FROM subtest1
     *  例如:
     *        select * from subtest where id > some (select name from subtest1);
     *    >/>= some ----> >/>= min
     *    </<= some ----> </<= max
     *    <>   some ----> not in
     *    =    some ----> in
     *    other  不改写
     */
    @Override
    public boolean visit(SQLSomeExpr x) {
    	
    	setSubQueryRelationOrFlag(x);
    	
    	List<SQLSelectItem> itemlist = ((SQLSelectQueryBlock)(x.getSubQuery().getQuery())).getSelectList();
    	SQLExpr sexpr = itemlist.get(0).getExpr();
    	
		if(x.getParent() instanceof SQLBinaryOpExpr){
			SQLBinaryOpExpr parentExpr = (SQLBinaryOpExpr)x.getParent();
			SQLAggregateExpr saexpr = null;
			switch (parentExpr.getOperator()) {
			case GreaterThan:
			case GreaterThanOrEqual:
			case NotLessThan:
				this.hasChange = true;
				if(sexpr instanceof SQLIdentifierExpr 
						|| (sexpr instanceof SQLPropertyExpr&&((SQLPropertyExpr)sexpr).getOwner() instanceof SQLIdentifierExpr)){
					saexpr = new SQLAggregateExpr("MIN");
					saexpr.getArguments().add(sexpr);
	        		saexpr.setParent(itemlist.get(0));
	        		itemlist.get(0).setExpr(saexpr);
				}
				SQLQueryExpr maxSubQuery = new SQLQueryExpr(x.getSubQuery());
        		x.getSubQuery().setParent(maxSubQuery);
        		// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
            	if(x.getParent() instanceof SQLBinaryOpExpr){
            		if(((SQLBinaryOpExpr)x.getParent()).getLeft().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setLeft(maxSubQuery);
            		}else if(((SQLBinaryOpExpr)x.getParent()).getRight().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setRight(maxSubQuery);
            		}
            	}
            	addSubQuerys(x.getSubQuery());
            	return super.visit(x.getSubQuery());
			case LessThan:
			case LessThanOrEqual:
			case NotGreaterThan:
				this.hasChange = true;
				if(sexpr instanceof SQLIdentifierExpr 
						|| (sexpr instanceof SQLPropertyExpr&&((SQLPropertyExpr)sexpr).getOwner() instanceof SQLIdentifierExpr)){
					saexpr = new SQLAggregateExpr("MAX");
					saexpr.getArguments().add(sexpr);
	        		saexpr.setParent(itemlist.get(0));
	        		itemlist.get(0).setExpr(saexpr);
				}
				// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
            	SQLQueryExpr minSubQuery = new SQLQueryExpr(x.getSubQuery());
        		x.getSubQuery().setParent(minSubQuery);
            	
            	if(x.getParent() instanceof SQLBinaryOpExpr){
            		if(((SQLBinaryOpExpr)x.getParent()).getLeft().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setLeft(minSubQuery);
            		}else if(((SQLBinaryOpExpr)x.getParent()).getRight().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setRight(minSubQuery);
            		}
            	}
            	addSubQuerys(x.getSubQuery());
            	return super.visit(x.getSubQuery());
			 case LessThanOrGreater:
			 case NotEqual:
				 this.hasChange = true;
					SQLInSubQueryExpr notInSubQueryExpr = new SQLInSubQueryExpr(x.getSubQuery());
					x.getSubQuery().setParent(notInSubQueryExpr);
					notInSubQueryExpr.setNot(true);
					// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
					if(x.getParent() instanceof SQLBinaryOpExpr){
						SQLBinaryOpExpr xp = (SQLBinaryOpExpr)x.getParent();
						
						if(xp.getLeft().equals(x)){
							notInSubQueryExpr.setExpr(xp.getRight());
						}else if(xp.getRight().equals(x)){
							notInSubQueryExpr.setExpr(xp.getLeft());
						}
						
						if(xp.getParent() instanceof MySqlSelectQueryBlock){
							((MySqlSelectQueryBlock)xp.getParent()).setWhere(notInSubQueryExpr);
						}else if(xp.getParent() instanceof SQLBinaryOpExpr){
							SQLBinaryOpExpr pp = ((SQLBinaryOpExpr)xp.getParent());
							if(pp.getLeft().equals(xp)){
								pp.setLeft(notInSubQueryExpr);
							}else if(pp.getRight().equals(xp)){
								pp.setRight(notInSubQueryExpr);
							}
						}
		            }
					addSubQuerys(x.getSubQuery());
		            return super.visit(notInSubQueryExpr);
			 case Equality:
				 this.hasChange = true;
				SQLInSubQueryExpr inSubQueryExpr = new SQLInSubQueryExpr(x.getSubQuery());
				x.getSubQuery().setParent(inSubQueryExpr);
				inSubQueryExpr.setNot(false);
				// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
				if(x.getParent() instanceof SQLBinaryOpExpr){
					SQLBinaryOpExpr xp = (SQLBinaryOpExpr)x.getParent();
					
					if(xp.getLeft().equals(x)){
						inSubQueryExpr.setExpr(xp.getRight());
					}else if(xp.getRight().equals(x)){
						inSubQueryExpr.setExpr(xp.getLeft());
					}
					
					if(xp.getParent() instanceof MySqlSelectQueryBlock){
						((MySqlSelectQueryBlock)xp.getParent()).setWhere(inSubQueryExpr);
					}else if(xp.getParent() instanceof SQLBinaryOpExpr){
						SQLBinaryOpExpr pp = ((SQLBinaryOpExpr)xp.getParent());
						if(pp.getLeft().equals(xp)){
							pp.setLeft(inSubQueryExpr);
						}else if(pp.getRight().equals(xp)){
							pp.setRight(inSubQueryExpr);
						}
					}
	            }
				addSubQuerys(x.getSubQuery());
	            return super.visit(inSubQueryExpr);
			 default:
				break;
			}
		}
		addSubQuerys(x.getSubQuery());
    	return super.visit(x);
    }

    /* 
     *  遇到 any 将子查询改写成  SELECT MIN(name) FROM subtest1
     *  例如:
     *    select * from subtest where id oper any (select name from subtest1);
     *    >/>= any ----> >/>= min
     *    </<= any ----> </<= max
     *    <>   any ----> not in
     *    =    some ----> in
     *    other  不改写
     */
    @Override
    public boolean visit(SQLAnyExpr x) {
    	
    	setSubQueryRelationOrFlag(x);
    	
    	List<SQLSelectItem> itemlist = ((SQLSelectQueryBlock)(x.getSubQuery().getQuery())).getSelectList();
    	SQLExpr sexpr = itemlist.get(0).getExpr();
    	
		if(x.getParent() instanceof SQLBinaryOpExpr){
			SQLBinaryOpExpr parentExpr = (SQLBinaryOpExpr)x.getParent();
			SQLAggregateExpr saexpr = null;
			switch (parentExpr.getOperator()) {
			case GreaterThan:
			case GreaterThanOrEqual:
			case NotLessThan:
				this.hasChange = true;
				if(sexpr instanceof SQLIdentifierExpr 
						|| (sexpr instanceof SQLPropertyExpr&&((SQLPropertyExpr)sexpr).getOwner() instanceof SQLIdentifierExpr)){
					saexpr = new SQLAggregateExpr("MIN");
					saexpr.getArguments().add(sexpr);
	        		saexpr.setParent(itemlist.get(0));
	        		itemlist.get(0).setExpr(saexpr);
				}
				SQLQueryExpr maxSubQuery = new SQLQueryExpr(x.getSubQuery());
        		x.getSubQuery().setParent(maxSubQuery);
				// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
            	if(x.getParent() instanceof SQLBinaryOpExpr){
            		if(((SQLBinaryOpExpr)x.getParent()).getLeft().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setLeft(maxSubQuery);
            		}else if(((SQLBinaryOpExpr)x.getParent()).getRight().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setRight(maxSubQuery);
            		}
            	}
            	addSubQuerys(x.getSubQuery());
            	return super.visit(x.getSubQuery());
			case LessThan:
			case LessThanOrEqual:
			case NotGreaterThan:
				this.hasChange = true;
				if(sexpr instanceof SQLIdentifierExpr 
						|| (sexpr instanceof SQLPropertyExpr&&((SQLPropertyExpr)sexpr).getOwner() instanceof SQLIdentifierExpr)){
					saexpr = new SQLAggregateExpr("MAX");
					saexpr.getArguments().add(sexpr);
	        		saexpr.setParent(itemlist.get(0));
	        		itemlist.get(0).setExpr(saexpr);
				}
				// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
            	SQLQueryExpr minSubQuery = new SQLQueryExpr(x.getSubQuery());
            	x.subQuery.setParent(minSubQuery);
            	if(x.getParent() instanceof SQLBinaryOpExpr){
            		if(((SQLBinaryOpExpr)x.getParent()).getLeft().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setLeft(minSubQuery);
            		}else if(((SQLBinaryOpExpr)x.getParent()).getRight().equals(x)){
            			((SQLBinaryOpExpr)x.getParent()).setRight(minSubQuery);
            		}
            	}
            	addSubQuerys(x.getSubQuery());
            	return super.visit(x.getSubQuery());
			 case LessThanOrGreater:
			 case NotEqual:
				 this.hasChange = true;
					SQLInSubQueryExpr notInSubQueryExpr = new SQLInSubQueryExpr(x.getSubQuery());
					x.getSubQuery().setParent(notInSubQueryExpr);
					notInSubQueryExpr.setNot(true);
					// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
					if(x.getParent() instanceof SQLBinaryOpExpr){
						SQLBinaryOpExpr xp = (SQLBinaryOpExpr)x.getParent();
						
						if(xp.getLeft().equals(x)){
							notInSubQueryExpr.setExpr(xp.getRight());
						}else if(xp.getRight().equals(x)){
							notInSubQueryExpr.setExpr(xp.getLeft());
						}
						
						if(xp.getParent() instanceof MySqlSelectQueryBlock){
							((MySqlSelectQueryBlock)xp.getParent()).setWhere(notInSubQueryExpr);
						}else if(xp.getParent() instanceof SQLBinaryOpExpr){
							SQLBinaryOpExpr pp = ((SQLBinaryOpExpr)xp.getParent());
							if(pp.getLeft().equals(xp)){
								pp.setLeft(notInSubQueryExpr);
							}else if(pp.getRight().equals(xp)){
								pp.setRight(notInSubQueryExpr);
							}
						}
		            }
					addSubQuerys(x.getSubQuery());
		            return super.visit(notInSubQueryExpr);
			 case Equality:
				 this.hasChange = true;
				SQLInSubQueryExpr inSubQueryExpr = new SQLInSubQueryExpr(x.getSubQuery());
				x.getSubQuery().setParent(inSubQueryExpr);
				inSubQueryExpr.setNot(false);
				// 生成新的SQLQueryExpr 替换当前 SQLAllExpr 节点
				if(x.getParent() instanceof SQLBinaryOpExpr){
					SQLBinaryOpExpr xp = (SQLBinaryOpExpr)x.getParent();
					
					if(xp.getLeft().equals(x)){
						inSubQueryExpr.setExpr(xp.getRight());
					}else if(xp.getRight().equals(x)){
						inSubQueryExpr.setExpr(xp.getLeft());
					}
					
					if(xp.getParent() instanceof MySqlSelectQueryBlock){
						((MySqlSelectQueryBlock)xp.getParent()).setWhere(inSubQueryExpr);
					}else if(xp.getParent() instanceof SQLBinaryOpExpr){
						SQLBinaryOpExpr pp = ((SQLBinaryOpExpr)xp.getParent());
						if(pp.getLeft().equals(xp)){
							pp.setLeft(inSubQueryExpr);
						}else if(pp.getRight().equals(xp)){
							pp.setRight(inSubQueryExpr);
						}
					}
	            }
				addSubQuerys(x.getSubQuery());
	            return super.visit(inSubQueryExpr);
			 default:
				break;
			}
		}
		addSubQuerys(x.getSubQuery());
    	return super.visit(x);
    }
    
    @Override
	public boolean visit(SQLBinaryOpExpr x) {
        x.getLeft().setParent(x);
        x.getRight().setParent(x);
        
        /*
         * fix bug 当 selectlist 存在多个子查询时, 主表没有别名的情况下.主表的查询条件 被错误的附加到子查询上.
         *  eg. select (select id from subtest2 where id = 1), (select id from subtest3 where id = 2) from subtest1 where id =4;
         *  像这样的子查询, subtest1 的 过滤条件  id = 4 .  被 加入到  subtest3 上. 加别名的情况下正常,不加别名,就会存在这个问题.
         *  这里设置好操作的是哪张表后,再进行判断.
         */
        String currenttable = x.getParent()==null?null: (String) x.getParent().getAttribute(SchemaStatVisitor.ATTR_TABLE);
        if(currenttable!=null){
        	this.setCurrentTable(currenttable);
        }
        
        switch (x.getOperator()) {
            case Equality:
            case LessThanOrEqualOrGreaterThan:
            case Is:
            case IsNot:
            case GreaterThan:
            case GreaterThanOrEqual:
            case LessThan:
            case LessThanOrEqual:
            case NotLessThan:
            case LessThanOrGreater:
			case NotEqual:
			case NotGreaterThan:
                handleCondition(x.getLeft(), x.getOperator().name, x.getRight());
                handleCondition(x.getRight(), x.getOperator().name, x.getLeft());
                handleRelationship(x.getLeft(), x.getOperator().name, x.getRight());
                break;
            case BooleanOr:
            	//永真条件，where条件抛弃
            	if(!RouterUtil.isConditionAlwaysTrue(x)) {
            		hasOrCondition = true;
            		
            		WhereUnit whereUnit = null;
            		if(conditions.size() > 0) {
            			whereUnit = new WhereUnit();
            			whereUnit.setFinishedParse(true);
            			whereUnit.addOutConditions(getConditions());
            			WhereUnit innerWhereUnit = new WhereUnit(x);
            			whereUnit.addSubWhereUnit(innerWhereUnit);
            		} else {
            			whereUnit = new WhereUnit(x);
            			whereUnit.addOutConditions(getConditions());
            		}
            		whereUnits.add(whereUnit);
            	}
            	return false;
            case Like:
            case NotLike:
            default:
                break;
        }
        return true;
    }
	
	/**
	 * 分解条件
	 */
	public List<List<Condition>> splitConditions() {
		//按照or拆分
		for(WhereUnit whereUnit : whereUnits) {
			splitUntilNoOr(whereUnit);
		}
		
		this.storedwhereUnits.addAll(whereUnits);
		
		loopFindSubWhereUnit(whereUnits);
		
		//拆分后的条件块解析成Condition列表
		for(WhereUnit whereUnit : storedwhereUnits) {
			this.getConditionsFromWhereUnit(whereUnit);
		}
		
		//多个WhereUnit组合:多层集合的组合
		return mergedConditions();
	}
	
	/**
	 * 循环寻找子WhereUnit（实际是嵌套的or）
	 * @param whereUnitList
	 */
	private void loopFindSubWhereUnit(List<WhereUnit> whereUnitList) {
		List<WhereUnit> subWhereUnits = new ArrayList<WhereUnit>();
		for(WhereUnit whereUnit : whereUnitList) {
			if(whereUnit.getSplitedExprList().size() > 0) {
				List<SQLExpr> removeSplitedList = new ArrayList<SQLExpr>();
				for(SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
					reset();
					if(isExprHasOr(sqlExpr)) {
						removeSplitedList.add(sqlExpr);
						WhereUnit subWhereUnit = this.whereUnits.get(0);
						splitUntilNoOr(subWhereUnit);
						whereUnit.addSubWhereUnit(subWhereUnit);
						subWhereUnits.add(subWhereUnit);
					} else {
						this.conditions.clear();
					}
				}
				if(removeSplitedList.size() > 0) {
					whereUnit.getSplitedExprList().removeAll(removeSplitedList);
				}
			}
			subWhereUnits.addAll(whereUnit.getSubWhereUnit());
		}
		if(subWhereUnits.size() > 0) {
			loopFindSubWhereUnit(subWhereUnits);
		}
	}
	
	private boolean isExprHasOr(SQLExpr expr) {
		expr.accept(this);
		return hasOrCondition;
	}
	
	private List<List<Condition>> mergedConditions() {
		if(storedwhereUnits.size() == 0) {
			return new ArrayList<List<Condition>>();
		}
		for(WhereUnit whereUnit : storedwhereUnits) {
			mergeOneWhereUnit(whereUnit);
		}
		return getMergedConditionList(storedwhereUnits);
		
	}
	
	/**
	 * 一个WhereUnit内递归
	 * @param whereUnit
	 */
	private void mergeOneWhereUnit(WhereUnit whereUnit) {
		if(whereUnit.getSubWhereUnit().size() > 0) {
			for(WhereUnit sub : whereUnit.getSubWhereUnit()) {
				mergeOneWhereUnit(sub);
			}
			
			if(whereUnit.getSubWhereUnit().size() > 1) {
				List<List<Condition>> mergedConditionList = getMergedConditionList(whereUnit.getSubWhereUnit());
				if(whereUnit.getOutConditions().size() > 0) {
					for(int i = 0; i < mergedConditionList.size() ; i++) {
						mergedConditionList.get(i).addAll(whereUnit.getOutConditions());
					}
				}
				whereUnit.setConditionList(mergedConditionList);
			} else if(whereUnit.getSubWhereUnit().size() == 1) {
				if(whereUnit.getOutConditions().size() > 0 && whereUnit.getSubWhereUnit().get(0).getConditionList().size() > 0) {
					for(int i = 0; i < whereUnit.getSubWhereUnit().get(0).getConditionList().size() ; i++) {
						whereUnit.getSubWhereUnit().get(0).getConditionList().get(i).addAll(whereUnit.getOutConditions());
					}
				}
				whereUnit.getConditionList().addAll(whereUnit.getSubWhereUnit().get(0).getConditionList());
			}
		} else {
			//do nothing
		}
	}
	
	/**
	 * 条件合并：多个WhereUnit中的条件组合
	 * @return
	 */
	private List<List<Condition>> getMergedConditionList(List<WhereUnit> whereUnitList) {
		List<List<Condition>> mergedConditionList = new ArrayList<List<Condition>>();
		if(whereUnitList.size() == 0) {
			return mergedConditionList; 
		}
		mergedConditionList.addAll(whereUnitList.get(0).getConditionList());
		
		for(int i = 1; i < whereUnitList.size(); i++) {
			mergedConditionList = merge(mergedConditionList, whereUnitList.get(i).getConditionList());
		}
		return mergedConditionList;
	}
	
	/**
	 * 两个list中的条件组合
	 * @param list1
	 * @param list2
	 * @return
	 */
	    private List<List<Condition>> merge(List<List<Condition>> list1, List<List<Condition>> list2) {
        if(list1.size() == 0) {
            return list2;
        } else if (list2.size() == 0) {
            return list1;
        }
        
		List<List<Condition>> retList = new ArrayList<List<Condition>>();
		for(int i = 0; i < list1.size(); i++) {
			for(int j = 0; j < list2.size(); j++) {
//				List<Condition> listTmp = new ArrayList<Condition>();
//				listTmp.addAll(list1.get(i));
//				listTmp.addAll(list2.get(j));
//				retList.add(listTmp);
			    /**
		         * 单纯做笛卡尔积运算，会导致非常多不必要的条件列表，</br>
		         * 当whereUnit和条件相对多时，会急剧增长条件列表项，内存直线上升，导致假死状态</br>
		         * 因此，修改算法为 </br>
		         * 1、先合并两个条件列表的元素为一个条件列表</br>
		         * 2、计算合并后的条件列表，在结果retList中：</br>
		         * &nbsp;2-1、如果当前的条件列表 是 另外一个条件列表的 超集，更新，并标识已存在</br>
		         * &nbsp;2-2、如果当前的条件列表 是 另外一个条件列表的 子集，标识已存在</br>
		         * 3、最后，如果被标识不存在，加入结果retList，否则丢弃。</br>
		         * 
		         * @author SvenAugustus
		         */
  			    // 合并两个条件列表的元素为一个条件列表
                List<Condition> listTmp = mergeSqlConditionList(list1.get(i), list2.get(j));
      
                // 判定当前的条件列表 是否 另外一个条件列表的 子集
                boolean exists = false;
                Iterator<List<Condition>> it = retList.iterator();
                while (it.hasNext()) {
                  List<Condition> result = (List<Condition>) it.next();
                  if (result != null && listTmp != null && listTmp.size() > result.size()) {
                    // 如果当前的条件列表 是 另外一个条件列表的 超集，更新，并标识已存在
                    if (sqlConditionListInOther(result, listTmp)) {
                      result.clear();
                      result.addAll(listTmp);
                      exists = true;
                      break;
                    }
                  } else {
                    // 如果当前的条件列表 是 另外一个条件列表的 子集，标识已存在
                    if (sqlConditionListInOther(listTmp, result)) {
                      exists = true;
                      break;
                    }
                  }
                }
                if (!exists) {// 被标识不存在，加入
                  retList.add(listTmp);
                } // 否则丢弃
			}
		}
        return retList;
    }
	
	private void getConditionsFromWhereUnit(WhereUnit whereUnit) {
		List<List<Condition>> retList = new ArrayList<List<Condition>>();
		//or语句外层的条件:如where condition1 and (condition2 or condition3),condition1就会在外层条件中,因为之前提取
		List<Condition> outSideCondition = new ArrayList<Condition>();
//		stashOutSideConditions();
		outSideCondition.addAll(conditions);
		this.conditions.clear();
		for(SQLExpr sqlExpr : whereUnit.getSplitedExprList()) {
			sqlExpr.accept(this);
//            List<Condition> conditions = new ArrayList<Condition>();
//            conditions.addAll(getConditions()); conditions.addAll(outSideCondition);
          /**
           * 合并两个条件列表的元素为一个条件列表，减少不必要多的条件项</br>
           * 
           * @author SvenAugustus
           */
          List<Condition> conditions = mergeSqlConditionList(getConditions(), outSideCondition);
			retList.add(conditions);
			this.conditions.clear();
		}
		whereUnit.setConditionList(retList);
		
		for(WhereUnit subWhere : whereUnit.getSubWhereUnit()) {
			getConditionsFromWhereUnit(subWhere);
		}
	}
	
	/**
	 * 递归拆分OR
	 * 
	 * @param whereUnit
	 * TODO:考虑嵌套or语句，条件中有子查询、 exists等很多种复杂情况是否能兼容
	 */
	private void splitUntilNoOr(WhereUnit whereUnit) {
		if(whereUnit.isFinishedParse()) {
			if(whereUnit.getSubWhereUnit().size() > 0) {
				for(int i = 0; i < whereUnit.getSubWhereUnit().size(); i++) {
					splitUntilNoOr(whereUnit.getSubWhereUnit().get(i));
				}
			} 
		} else {
			SQLBinaryOpExpr expr = whereUnit.getCanSplitExpr();
			if(expr.getOperator() == SQLBinaryOperator.BooleanOr) {
//				whereUnit.addSplitedExpr(expr.getRight());
				addExprIfNotFalse(whereUnit, expr.getRight());
				if(expr.getLeft() instanceof SQLBinaryOpExpr) {
					whereUnit.setCanSplitExpr((SQLBinaryOpExpr)expr.getLeft());
					splitUntilNoOr(whereUnit);
				} else {
					addExprIfNotFalse(whereUnit, expr.getLeft());
				}
			} else {
				addExprIfNotFalse(whereUnit, expr);
				whereUnit.setFinishedParse(true);
			}
		}
    }

	private void addExprIfNotFalse(WhereUnit whereUnit, SQLExpr expr) {
		//非永假条件加入路由计算
		if(!RouterUtil.isConditionAlwaysFalse(expr)) {
			whereUnit.addSplitedExpr(expr);
		}
	}
	
	@Override
    public boolean visit(SQLAlterTableStatement x) {
        String tableName = x.getName().toString();
        TableStat stat = getTableStat(tableName,tableName);
        stat.incrementAlterCount();

        setCurrentTable(x, tableName);

        for (SQLAlterTableItem item : x.getItems()) {
            item.setParent(x);
            item.accept(this);
        }

        return false;
    }
    public boolean visit(MySqlCreateTableStatement x) {
        SQLName sqlName=  x.getName();
        if(sqlName!=null)
        {
            String table = sqlName.toString();
            if(table.startsWith("`"))
            {
                table=table.substring(1,table.length()-1);
            }
            setCurrentTable(table);
        }
        return false;
    }
    public boolean visit(MySqlInsertStatement x) {
        SQLName sqlName=  x.getTableName();
        if(sqlName!=null)
        {
            String table = sqlName.toString();
            if(table.startsWith("`"))
            {
                table=table.substring(1,table.length()-1);
            }
            setCurrentTable(sqlName.toString());
        }
        return false;
    }
	// DUAL
    public boolean visit(MySqlDeleteStatement x) {
        setAliasMap();

        setMode(x, Mode.Delete);

        accept(x.getFrom());
        accept(x.getUsing());
        x.getTableSource().accept(this);

        if (x.getTableSource() instanceof SQLExprTableSource) {
            SQLName tableName = (SQLName) ((SQLExprTableSource) x.getTableSource()).getExpr();
            String ident = tableName.toString();
            setCurrentTable(x, ident);

            TableStat stat = this.getTableStat(ident,ident);
            stat.incrementDeleteCount();
        }

        accept(x.getWhere());

        accept(x.getOrderBy());
        accept(x.getLimit());

        return false;
    }
    
    public void endVisit(MySqlDeleteStatement x) {
    }
    
    public boolean visit(SQLUpdateStatement x) {
        setAliasMap();

        setMode(x, Mode.Update);

        SQLName identName = x.getTableName();
        if (identName != null) {
            String ident = identName.toString();
            String alias = x.getTableSource().getAlias();
            setCurrentTable(ident);

            TableStat stat = getTableStat(ident);
            stat.incrementUpdateCount();

            Map<String, String> aliasMap = getAliasMap();
            
            aliasMap.put(ident, ident);
            if(alias != null) {
            	aliasMap.put(alias, ident);
            }
        } else {
            x.getTableSource().accept(this);
        }

        accept(x.getItems());
        accept(x.getWhere());

        return false;
    }
    
    @Override
    public void endVisit(MySqlHintStatement x) {
    	super.endVisit(x);
    }
    
    @Override
    public boolean visit(MySqlHintStatement x) {
    	List<SQLCommentHint> hits = x.getHints();
    	if(hits != null && !hits.isEmpty()) {
    		String schema = parseSchema(hits);
    		if(schema != null ) {
    			setCurrentTable(x, schema + ".");
    			return true;
    		}
    	}
    	return true;
    }
    
    private String parseSchema(List<SQLCommentHint> hits) {
    	String regx = "\\!mycat:schema\\s*=([\\s\\w]*)$";
    	for(SQLCommentHint hit : hits ) {
    		Pattern pattern = Pattern.compile(regx);
    		Matcher m = pattern.matcher(hit.getText());
    		if(m.matches()) {
    			return m.group(1).trim();
    		}
    	}
		return null;
    }

    public Queue<SQLSelect> getSubQuerys() {
		return subQuerys;
	}
	
	private void addSubQuerys(SQLSelect sqlselect){
		/* 多个 sqlselect 之间  , equals 和 hashcode 是相同的.去重时 都被过滤掉了. */
		if(subQuerys.isEmpty()){
			subQuerys.add(sqlselect);
			return;
		}
        boolean exists = false;
		Iterator<SQLSelect> iter = subQuerys.iterator();
		while(iter.hasNext()){
			SQLSelect ss = iter.next();
			if(ss.getQuery() instanceof SQLSelectQueryBlock
					&&sqlselect.getQuery() instanceof SQLSelectQueryBlock){
				SQLSelectQueryBlock current = (SQLSelectQueryBlock)sqlselect.getQuery();
				SQLSelectQueryBlock ssqb = (SQLSelectQueryBlock)ss.getQuery();
//                  if(!sqlSelectQueryBlockEquals(ssqb,current)){
//                    subQuerys.add(sqlselect);
//                  }
                /**
                 * 修正判定逻辑，应改为全不在subQuerys中才加入<br/>
                 * 
                 * @author SvenAugustus
                 */
                if(sqlSelectQueryBlockEquals(current,ssqb)){
                   exists = true;
                   break;
                }
				}
			}
        if(!exists) {
          subQuerys.add(sqlselect);
		}
	}
	
	/* 多个 sqlselect 之间  , equals 和 hashcode 是相同的.去重时 使用 SQLSelectQueryBlock equals 方法 */
    private boolean sqlSelectQueryBlockEquals(SQLSelectQueryBlock obj1,SQLSelectQueryBlock obj2) {
        if (obj1 == obj2) return true;
        if (obj2 == null) return false;
        if (obj1.getClass() != obj2.getClass()) return false;
        if (obj1.isParenthesized() ^ obj2.isParenthesized()) return false;
        if (obj1.getDistionOption() != obj2.getDistionOption()) return false;
        if (obj1.getFrom() == null) {
            if (obj2.getFrom() != null) return false;
        } else if (!obj1.getFrom().equals(obj2.getFrom())) return false;
        if (obj1.getGroupBy() == null) {
            if (obj2.getGroupBy() != null) return false;
        } else if (!obj1.getGroupBy().equals(obj2.getGroupBy())) return false;
        if (obj1.getInto() == null) {
            if (obj2.getInto() != null) return false;
        } else if (!obj1.getInto().equals(obj2.getInto())) return false;
        if (obj1.getSelectList() == null) {
            if (obj2.getSelectList() != null) return false;
        } else if (!obj1.getSelectList().equals(obj2.getSelectList())) return false;
        if (obj1.getWhere() == null) {
            if (obj2.getWhere() != null) return false;
        } else if (!obj1.getWhere().equals(obj2.getWhere())) return false;
        return true;
    }

	public boolean isHasChange() {
		return hasChange;
	}

	public boolean isSubqueryRelationOr() {
		return subqueryRelationOr;
	}
    
    /**
     * 判定当前的条件列表 是否 另外一个条件列表的 子集
     * 
     * @author SvenAugustus
     * @param current 当前的条件列表 
     * @param other 另外一个条件列表
     * @return
     */
    private boolean sqlConditionListInOther(List<Condition> current, List<Condition> other) {
      if (current == null) {
        if (other != null) {
          return false;
        }
        return true;
      }
      if (current.size() > other.size()) {
        return false;
      }
      if (other.size() == current.size()) {
        // 判定两个条件列表的元素是否内容相等
        return sqlConditionListEquals(current, other);
      }
      for (int j = 0; j < current.size(); j++) {
        boolean exists = false;
        for (int i = 0; i < other.size(); i++) {
          // 判定两个条件是否相等
          if (sqlConditionEquals(current.get(j), other.get(i))) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          return false;
        }
      }
      return true;
    }
    
    /**
     * 判定两个条件列表的元素是否内容相等
     * 
     * @author SvenAugustus
     * @param list1
     * @param list2
     * @return
     */
    private boolean sqlConditionListEquals(List<Condition> list1, List<Condition> list2) {
      if (list1 == null) {
        if (list2 != null) {
          return false;
        }
        return true;
      }
      if (list2.size() != list1.size()) {
        return false;
      }
      int len = list1.size();
      for (int j = 0; j < len; j++) {
        boolean exists = false;
        for (int i = 0; i < len; i++) {
          // 判定两个条件是否相等
          if (sqlConditionEquals(list2.get(j), list1.get(i))) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          return false;
        }
      }
      return true;
    }

    /**
     * 合并两个条件列表的元素为一个条件列表
     * 
     * @author SvenAugustus
     * @param list1 条件列表1
     * @param list2 条件列表2
     * @return
     */
    private List<Condition> mergeSqlConditionList(List<Condition> list1, List<Condition> list2) {
      if (list1 == null) {
        list1 = new ArrayList();
      }
      if (list2 == null) {
        list2 = new ArrayList();
      }
      List<Condition> retList = new ArrayList<Condition>();
      if (!list1.isEmpty() && !(list1.get(0) instanceof Condition)) {
        return retList;
      }
      if (!list2.isEmpty() && !(list2.get(0) instanceof Condition)) {
        return retList;
      }
      retList.addAll(list1);
      for (int j = 0; j < list2.size(); j++) {
        boolean exists = false;
        for (int i = 0; i < list1.size(); i++) {
          if (sqlConditionEquals(list2.get(j), list1.get(i))) {
            exists = true;
            break;
          }
        }
        if (!exists) {
          retList.add(list2.get(j));
        }
      }
      return retList;
    }
    
    /**
     * 判定两个条件是否相等
     * 
     * @author SvenAugustus
     * @param obj1
     * @param obj2
     * @return
     */
    private boolean sqlConditionEquals(Condition obj1, Condition obj2) {
      if (obj1 == obj2) {
        return true;
      }
      if (obj2 == null) {
        return false;
      }
      if (obj1.getClass() != obj2.getClass()) {
        return false;
      }
      Condition other = (Condition) obj2;
      if (obj1.getColumn() == null) {
        if (other.getColumn() != null) {
          return false;
        }
      } else if (!obj1.getColumn().equals(other.getColumn())) {
        return false;
      }
      if (obj1.getOperator() == null) {
        if (other.getOperator() != null) {
          return false;
        }
      } else if (!obj1.getOperator().equals(other.getOperator())) {
        return false;
      }
      if (obj1.getValues() == null) {
        if (other.getValues() != null) {
          return false;
        }
      } else {
        boolean notEquals=false;
        for (Object val1: obj1.getValues()) {
          for (Object val2: obj2.getValues()) {
            if(val1==null) {
              if(val2!=null) {
                notEquals=true;
                break;
              }
            }else if(!val1.equals(val2)) {
              notEquals=true;
              break;
            }
          }
          if(notEquals)break;
        }
        if(notEquals)
        return false;
      }
      return true;
    }
}
