package io.mycat.catlets;


import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**  
 * 功能详细描述:分片join,解析join语句
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2015年01月25日 
 * @version 0.0.1
 */
public class JoinParser {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(JoinParser.class);

    private MySqlSelectQueryBlock mysqlQuery;
    private String stmt = "";
    private String joinType;
    private String masterTable;
    private TableFilter tableFilter;
    
    //private LinkedHashMap<String,String> fieldAliasMap = new LinkedHashMap<String,String>();
    
	public JoinParser(MySqlSelectQueryBlock selectQuery,String stmt) {
		this.mysqlQuery=selectQuery;
		this.stmt=stmt;
	}

    public void parser() {
        masterTable = "";

        SQLTableSource table = mysqlQuery.getFrom();
        parserTable(table, tableFilter, false);

        parserFields(mysqlQuery.getSelectList());
        parserMasterTable();

        parserWhere(mysqlQuery.getWhere(), "");
        // getJoinField();
        parserOrderBy(mysqlQuery.getOrderBy());
        parserLimit();
        // LOGGER.info("field "+fieldAliasMap);
        // LOGGER.info("master "+masterTable);
        //  LOGGER.info("join Lkey "+getJoinLkey());
        //  LOGGER.info("join Rkey "+getJoinRkey());
        LOGGER.info("SQL: " + this.stmt);
    }

    /**
     * 解析 table，生成 {@link #tableFilter}
     *
     * @param table table
     * @param tFilter 表过滤，即连接条件
     * @param isOutJoin TODO 待读：用途
     */
    private void parserTable(SQLTableSource table, TableFilter tFilter, boolean isOutJoin) {
        if (table instanceof SQLJoinTableSource) {
            SQLJoinTableSource table1 = (SQLJoinTableSource) table;
            joinType = table1.getJoinType().toString();
            if ((table1.getJoinType() == JoinType.COMMA) || (table1.getJoinType() == JoinType.JOIN) || (table1.getJoinType() == JoinType.INNER_JOIN)
                    || (table1.getJoinType() == JoinType.LEFT_OUTER_JOIN)) {
                tFilter = setTableFilter(tFilter, getTableFilter(table1.getLeft(), isOutJoin));
                if (tableFilter == null) {
                    tableFilter = tFilter;
                }
            }
            //parserTable(table1.getLeft());	//SQLExprTableSource
            parserTable(table1.getRight(), tFilter, true);

            SQLExpr expr = table1.getCondition();//SQLBinaryOpExpr
            parserJoinKey(expr);
        } else {
            tFilter = setTableFilter(tFilter, getTableFilter(table, isOutJoin));
            LOGGER.info("table " + table.toString() + " Alias:" + table.getAlias() + " Hints:" + table.getHints());
        }
    }

    /**
     * 设置 tableFilter
     *
     * @param tFilter tableFilter
     * @param newFilter 下一个（new） tableFilter
     * @return tableFilter
     */
    private TableFilter setTableFilter(TableFilter tFilter, TableFilter newFilter) {
        if (tFilter == null) {
            tFilter = newFilter;
            return tFilter;
        } else {
            tFilter.setTableJoin(newFilter);
            return tFilter.getTableJoin();
        }
    }

    /**
     * 获得 tableFilter
     *
     * @param table table
     * @param isOutJoin TODO 待读
     * @return tableFilter
     */
    private TableFilter getTableFilter(SQLTableSource table, boolean isOutJoin) {
        String key;
        String value = table.toString().trim(); // TODO 拓展点：如果超过2个表join，这里要处理下。因为druid是基于向左不断拓展
        if (table.getAlias() == null) {
            key = value;
        } else {
            key = table.getAlias().trim();
        }
        return new TableFilter(value, key, isOutJoin);
    }

    /**
     * 解析 join on 后的表达式
     *
     * @param expr 表达式
     */
    private void parserJoinKey(SQLExpr expr) {
        if (expr == null) {
            return;
        }
        parserWhere(expr, "");
    }

    private String getExprFieldName(SQLAggregateExpr expr){
		StringBuilder field = new StringBuilder();
		for (SQLExpr item :expr.getArguments()){
			field.append(item.toString());
		}		
		return expr.getMethodName()+"("+field.toString()+")";
	}
	
	private String getFieldName(SQLSelectItem item){
		if (item.getExpr() instanceof SQLPropertyExpr) {			
			return item.getExpr().toString();//字段别名
		}
		else {
			return item.toString();
		}
	}
	
	private String getMethodInvokeFieldName(SQLSelectItem item){
		SQLMethodInvokeExpr invoke = (SQLMethodInvokeExpr)item.getExpr();
		List<SQLExpr> itemExprs = invoke.getParameters();
		for(SQLExpr itemExpr:itemExprs){
			if (itemExpr instanceof SQLPropertyExpr) {
				return itemExpr.toString();//字段别名
			}
		}
		return item.toString();
	}

    /**
     * 解析查询 fields
     *
     * @param mysqlSelectList fields
     */
    private void parserFields(List<SQLSelectItem> mysqlSelectList) {
        //显示的字段
        String key = "";
        String value = "";
        String exprfield = "";
        for (SQLSelectItem item : mysqlSelectList) {
            if (item.getExpr() instanceof SQLAllColumnExpr) { // select *
                setField(item.toString(), item.toString());
            } else {
                if (item.getExpr() instanceof SQLAggregateExpr) { // TODO 待读
                    SQLAggregateExpr expr = (SQLAggregateExpr) item.getExpr();
                    key = getExprFieldName(expr);
                    setField(key, value);
                } else if (item.getExpr() instanceof SQLMethodInvokeExpr) { // TODO 待读
                    key = getMethodInvokeFieldName(item);
                    exprfield = getFieldName(item);
//					value=item.getAlias();
                    setField(key, value, exprfield);
                } else {
                    key = getFieldName(item);
                    value = item.getAlias();
                    setField(key, value);
                }
            }
        }
    }

    private void setField(String key,String value){
		//fieldAliasMap.put(key, value);
		if (tableFilter!=null){
			tableFilter.addField(key, value);
		}
	}
	
	private void setField(String key,String value,String expr){
		//fieldAliasMap.put(key, value);
		if (tableFilter!=null){
			tableFilter.addField(key, value,expr);
		}
	}

    /**
     * 判断并获得主表
     */
    private void parserMasterTable() {
        if (tableFilter != null) {
            masterTable = tableFilter.getTableAlia();
        }
    }

    /**
     * 判断 value 是否是一个 field。例如，u.id = xxxx 这种。
     *
     * @param value 值
     * @return 是否
     */
    private boolean checkJoinField(String value) {
        if (value == null) {
            return false;
        } else {
            int i = value.indexOf('.'); // TODO bug：如果value是带"."的字符串会误判
            return i > 0;
        }
    }

    /**
     * 解析条件表达式，where 或者 join on 后的表达式
     *
     * @param aexpr 表达式
     * @param Operator 操作符
     */
    private void parserWhere(SQLExpr aexpr, String Operator) {
        if (aexpr == null) {
            return;
        }
        if (aexpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr expr = (SQLBinaryOpExpr) aexpr;
            SQLExpr exprL = expr.getLeft();
            if (!(exprL instanceof SQLBinaryOpExpr)) { // 单条条件表达式
                opSQLExpr((SQLBinaryOpExpr) aexpr, Operator);
            } else { // SQLBinaryOpExpr：例如，xxx = yyy AND zzz = aaa
                // if (expr.getOperator().getName().equals("AND")) {
                if (expr.getOperator() == SQLBinaryOperator.BooleanAnd) {
                    //parserWhere(exprL);
                    //parserWhere(expr.getRight());
                    andorWhere(exprL, expr.getOperator().getName(), expr.getRight());
                } else if (expr.getOperator() == SQLBinaryOperator.BooleanOr) {//.getName().equals("OR")) {
                    andorWhere(exprL, expr.getOperator().getName(), expr.getRight());
                } else {
                    throw new RuntimeException("Can't identify the operation of  of where");
                }
            }
        } else if (aexpr instanceof SQLInListExpr) {
            SQLInListExpr expr = (SQLInListExpr) aexpr;
            SQLExpr exprL = expr.getExpr();
            String field = exprL.toString();
            tableFilter.addWhere(field, SQLUtils.toMySqlString(expr), Operator);
        }
    }

    /**
     * 解析左右表达式
     *
     * @param exprL 左表达式
     * @param Operator 操作符
     * @param exprR 右表达式
     */
    private void andorWhere(SQLExpr exprL, String Operator, SQLExpr exprR) {
        parserWhere(exprL, "");
        parserWhere(exprR, Operator);
    }

    /**
     * 处理单条条件表达式。例如，xxx=yyy。不是 xxx=yyy and zzz=qqq 这种多条件表达式
     *
     * @param expr 表达式
     * @param Operator 操作符
     */
    private void opSQLExpr(SQLBinaryOpExpr expr, String Operator) {
        if (expr == null) {
            return;
        }
        SQLExpr exprL = expr.getLeft();
        if (!(exprL instanceof SQLBinaryOpExpr)) {
            String field = exprL.toString();
            String value = getExpValue(expr.getRight()).toString();
            if (expr.getOperator() == SQLBinaryOperator.Equality) {
                if (checkJoinField(value)) {
                    //joinLkey=field;
                    //joinRkey=value;
                    tableFilter.setJoinKey(field, value);
                } else {
                    tableFilter.addWhere(field, value, expr.getOperator().getName(), Operator);
                }
            } else {
                tableFilter.addWhere(field, value, expr.getOperator().getName(), Operator);
            }
        }
    }

	private Object getExpValue(SQLExpr expr){
		if (expr instanceof SQLIntegerExpr){
			return ((SQLIntegerExpr)expr).getNumber().intValue();
		}
		if (expr instanceof SQLNumberExpr){
			return ((SQLNumberExpr)expr).getNumber().doubleValue();
		}		
		if (expr instanceof SQLCharExpr){
			String va=((SQLCharExpr)expr).toString();
			return va;//remove(va,'\'');
		}
		if (expr instanceof SQLBooleanExpr){			
			return ((SQLBooleanExpr)expr).getValue();
		}			
		if (expr instanceof SQLNullExpr){
			return null;
		}
	
		return expr;		
	}

    /**
     * 解析排序条件
     *
     * @param orderby 排序条件
     */
    private void parserOrderBy(SQLOrderBy orderby) {
        if (orderby != null) {
            for (int i = 0; i < orderby.getItems().size(); i++) {
                SQLSelectOrderByItem orderitem = orderby.getItems().get(i);
                tableFilter.addOrders(orderitem.getExpr().toString(), getSQLExprToAsc(orderitem.getType()));
            }
        }
    }

    /**
     * 解析 limit 条件
     */
    private void parserLimit() {
        int limitoff = 0;
        int limitnum = 0;
        if (this.mysqlQuery.getLimit() != null) {
            limitoff = getSQLExprToInt(this.mysqlQuery.getLimit().getOffset());
            limitnum = getSQLExprToInt(this.mysqlQuery.getLimit().getRowCount());
            tableFilter.addLimit(limitoff, limitnum);
        }
    }

    private int getSQLExprToInt(SQLExpr expr) {
        if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getNumber().intValue();
        }
        return 0;
    }

    private String getSQLExprToAsc(SQLOrderingSpecification ASC) {
        if (ASC == null) {
            return " ASC ";
        }
        if (ASC == SQLOrderingSpecification.DESC) {
            return " DESC ";
        } else {
            return " ASC ";
        }
    }

    public String getChildSQL(){
		//String sql="select "+joinRkey+","+sql+" from "+mtable+" where "+joinRkey+" in ";
		String sql=tableFilter.getTableJoin().getSQL();
		return sql;
	}
	
	public String getSql(){
		stmt=tableFilter.getSQL();
		return stmt;
	}
	
	public String getJoinType(){
		return joinType;
	}
	public String getJoinLkey(){
		return tableFilter.getJoinKey(true);
	}	
	public String getJoinRkey(){
		return tableFilter.getJoinKey(false);
	}	
}
