package io.mycat.catlets;


import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

/**  
 * 功能详细描述:分片join,解析join语句
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2015年01月25日 
 * @version 0.0.1
 */


public class JoinParser {
	
	protected static final Logger LOGGER = LoggerFactory.getLogger(JoinParser.class);
	
    private MySqlSelectQueryBlock mysqlQuery;
    private String stmt="";
    private String joinType;
    private String masterTable;    
    private TableFilter tableFilter; // a table -> b table 的链表 
    
    //private LinkedHashMap<String,String> fieldAliasMap = new LinkedHashMap<String,String>();
    
	public JoinParser(MySqlSelectQueryBlock selectQuery,String stmt) {
		this.mysqlQuery=selectQuery;
		this.stmt=stmt;
	}
	
	public void parser(){
	   masterTable="";	   
	   
	   SQLTableSource table=mysqlQuery.getFrom();	 //a 表  
	   parserTable(table,tableFilter,false); // 组成链表
	   
	   parserFields(mysqlQuery.getSelectList());  //查询字段放到各个查询表中。
	   parserMasterTable();	 //查询主表 别名   
	   
	   parserWhere(mysqlQuery.getWhere(),""); // where 条件放到各个查询表中。	   
	 // getJoinField();
	   parserOrderBy(mysqlQuery.getOrderBy());  // order 条件放到各个查询表中。
	   parserLimit(); // limit 
//	   LOGGER.info("field "+fieldAliasMap);	  	   
//	   LOGGER.info("master "+masterTable);
//	   LOGGER.info("join Lkey "+getJoinLkey()); 
//	   LOGGER.info("join Rkey "+getJoinRkey()); 	   
	   LOGGER.info("SQL: "+this.stmt);
	}
	
	private void parserTable(SQLTableSource table,TableFilter tFilter,boolean isOutJoin){
		if(table instanceof SQLJoinTableSource){
			SQLJoinTableSource table1=(SQLJoinTableSource)table;	
			joinType=table1.getJoinType().toString();
			if ((table1.getJoinType()==JoinType.COMMA)||(table1.getJoinType()==JoinType.JOIN)||(table1.getJoinType()==JoinType.INNER_JOIN)
					||(table1.getJoinType()==JoinType.LEFT_OUTER_JOIN))	{					
				tFilter=setTableFilter(tFilter,getTableFilter(table1.getLeft(),isOutJoin));
				if (tableFilter==null){
					tableFilter=tFilter;
				}
			}
			//parserTable(table1.getLeft());	//SQLExprTableSource
			parserTable(table1.getRight(),tFilter,true);
			
			SQLExpr expr=table1.getCondition();//SQLBinaryOpExpr
			parserJoinKey(expr);
		}
		else {
			tFilter=setTableFilter(tFilter,getTableFilter(table,isOutJoin));
			LOGGER.info("table "+table.toString() +" Alias:"+table.getAlias()+" Hints:"+table.getHints());
		}
	}
	private TableFilter setTableFilter(TableFilter tFilter,TableFilter newFilter){
		if (tFilter==null) {
			tFilter=newFilter;
			return tFilter;
		}
		else {
			tFilter.setTableJoin(newFilter);	
			return tFilter.getTableJoin();
		}
	}
	private TableFilter getTableFilter(SQLTableSource table,boolean isOutJoin){	
		String key   ;
		String value = table.toString().trim();
		if (table.getAlias()==null) {
			key=value;
		}
		else {
			 key   = table.getAlias().trim();
		}
		return new TableFilter(value,key,isOutJoin);	
	}
	
	private void parserJoinKey(SQLExpr expr){		
		if (expr==null) {
			return;
		}
		 parserWhere(expr,"");
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
	
	
	private void parserFields(List<SQLSelectItem> mysqlSelectList){
		//显示的字段
		String key="";
		String value ="";
		String exprfield = "";
		for(SQLSelectItem item : mysqlSelectList) {
			if (item.getExpr() instanceof SQLAllColumnExpr) {
				//*解析
				setField(item.toString(), item.toString());
			}
			else {
				if (item.getExpr() instanceof SQLAggregateExpr) {
					SQLAggregateExpr expr =(SQLAggregateExpr)item.getExpr();
					 key = getExprFieldName(expr);
					 setField(key, value);
				}else if(item.getExpr() instanceof SQLMethodInvokeExpr){
					key = getMethodInvokeFieldName(item);
					exprfield=getFieldName(item);
//					value=item.getAlias();
					setField(key, value,exprfield);
				}else {					
					key=getFieldName(item);
					value=item.getAlias();
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
	
	
	//判断并获得主表
	private void parserMasterTable(){ 
		if (tableFilter!=null){
		   masterTable=tableFilter.getTableAlia();
		}
	}	

	private boolean checkJoinField(String value){
		if (value==null){
			return false;	
		}
		else {
			int i=value.indexOf('.');	
			return i>0;
		}
	}

	//解析 a.field = b.field 
	private void parserWhere(SQLExpr aexpr,String Operator){
		 if (aexpr==null) {
			 return;
		 }
	     if (aexpr instanceof SQLBinaryOpExpr){
		   SQLBinaryOpExpr expr=(SQLBinaryOpExpr)aexpr;  
		   SQLExpr exprL=expr.getLeft();
		   if (!(exprL instanceof SQLBinaryOpExpr))
		   {
			  opSQLExpr((SQLBinaryOpExpr)aexpr,Operator);			  
		   }
		   else {
			// if (expr.getOperator().getName().equals("AND")) { 
			 if (expr.getOperator()==SQLBinaryOperator.BooleanAnd) { 	 
			   //parserWhere(exprL); 
			   //parserWhere(expr.getRight());
			   andorWhere(exprL,expr.getOperator().getName(),expr.getRight());
			 }
			 else if (expr.getOperator()==SQLBinaryOperator.BooleanOr){//.getName().equals("OR")) {  
				andorWhere(exprL,expr.getOperator().getName(),expr.getRight()); 				
			 }
			 else {
				 throw new RuntimeException("Can't identify the operation of  of where"); 
			 }
		   }
	   }else if(aexpr instanceof SQLInListExpr){
		   SQLInListExpr expr = (SQLInListExpr)aexpr;
		   SQLExpr exprL =  expr.getExpr();
		   String field=exprL.toString();
		   tableFilter.addWhere(field, SQLUtils.toMySqlString(expr), Operator);
	   }
	     
	}
	
	private void andorWhere(SQLExpr exprL,String Operator,SQLExpr exprR ){ 
		   parserWhere(exprL,"");
		   parserWhere(exprR,Operator);
	}	
	   
    private void opSQLExpr(SQLBinaryOpExpr expr,String Operator) {
		   if (expr==null) {
			   return;
		   }
		   SQLExpr exprL=expr.getLeft();
		   if (!(exprL instanceof SQLBinaryOpExpr))
		   {
			   String field=exprL.toString(); //获取表达式 左边的值
			   String value=getExpValue(expr.getRight()).toString(); //获取表达式右边的值
			   if (expr.getOperator()==SQLBinaryOperator.Equality) {  
				 if (checkJoinField(value)) {//设置joinKey
					//joinLkey=field;
					//joinRkey=value; 
					tableFilter.setJoinKey(field,value);
				 }
				 else {
					 tableFilter.addWhere(field, value, expr.getOperator().getName(), Operator);
				 }
			   }
			   else {
				   tableFilter.addWhere(field, value, expr.getOperator().getName(), Operator);
			   }
		   }		
	}

	private Object getExpValue(SQLExpr expr){
		if (expr instanceof SQLIntegerExpr){
			return ((SQLIntegerExpr)expr).getNumber().longValue();
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
	
	private void parserOrderBy(SQLOrderBy orderby)   
    {   
		if (orderby != null ){
			for (int i = 0; i < orderby.getItems().size(); i++)
	        {
			  SQLSelectOrderByItem orderitem = orderby.getItems().get(i);
			  tableFilter.addOrders(i, orderitem.getExpr().toString(), getSQLExprToAsc(orderitem.getType()));
            }
		}		
    }  
	private void parserLimit(){
	  int limitoff=0;
	  int limitnum=0;
	  if (this.mysqlQuery.getLimit()!=null) {
	    limitoff=getSQLExprToInt(this.mysqlQuery.getLimit().getOffset());			
	    limitnum=getSQLExprToInt(this.mysqlQuery.getLimit().getRowCount());
	    tableFilter.addLimit(limitoff,limitnum);
	  }
	}
	
	private int getSQLExprToInt(SQLExpr expr){
		if (expr instanceof SQLIntegerExpr){
			return ((SQLIntegerExpr)expr).getNumber().intValue();
		}
		return 0;		
	}
	
	private String getSQLExprToAsc(SQLOrderingSpecification ASC){
		if (ASC==null ) {
			return " ASC ";
		}
		if (ASC==SQLOrderingSpecification.DESC){
			return " DESC ";
		}
		else {
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
	
	//返回a表排序的字段
	public LinkedHashMap<String, Integer> getOrderByCols(){
		return tableFilter.getOrderByCols();
	}
	//返回b表排序的字段
	public LinkedHashMap<String, Integer> getChildByCols(){
		return tableFilter.getTableJoin().getOrderByCols();
	}
	//是否有order 排序
	public boolean hasOrder() {
		return tableFilter.getOrderByCols().size() > 0 || tableFilter.getTableJoin().getOrderByCols().size() > 0;		
		
	}
	/*
	 * limit 的 start*/
	public int getOffset() {
		return tableFilter.getOffset();
	}

	/*
	 * limit 的 rowCount*/
	public int getRowCount() {
		return tableFilter.getRowCount();
	}
	/*
	 * 是否有limit 输出。
	 */
	public boolean hasLimit() {
		return tableFilter.getOffset() > 0 ||  tableFilter.getRowCount() > 0 ; 
	}

}
