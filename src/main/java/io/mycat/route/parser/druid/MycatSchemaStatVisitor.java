package io.mycat.route.parser.druid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.stat.TableStat.Condition;
import com.alibaba.druid.stat.TableStat.Mode;

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
				List<Condition> listTmp = new ArrayList<Condition>();
				listTmp.addAll(list1.get(i));
				listTmp.addAll(list2.get(j));
				retList.add(listTmp);
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
			List<Condition> conditions = new ArrayList<Condition>();
			conditions.addAll(getConditions());
			conditions.addAll(outSideCondition);
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
}
