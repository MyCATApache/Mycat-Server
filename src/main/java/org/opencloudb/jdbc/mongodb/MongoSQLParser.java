package org.opencloudb.jdbc.mongodb;



import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.*;
/**  
 * 功能详细描述
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2014年12月19日 下午6:50:23 
 * @version 0.0.1
 */
public class MongoSQLParser {
	private   final DB _db;
	private   final String _sql;
	private   final SQLStatement statement;	
	private   List _params;
	private   int _pos;	
	public MongoSQLParser(DB db, String sql)  throws MongoSQLException
	   {
	     this._db = db;
	     this._sql = sql;
	     this.statement = parser(sql);
	   }
	
	public SQLStatement parser(String s) throws MongoSQLException
	   {
	     s = s.trim();
	     try
	     {
	        MySqlStatementParser parser = new MySqlStatementParser(s);
	        return parser.parseStatement();
	     }
	     catch (Exception e)
	     {
	       e.printStackTrace();
	    }
	     throw new MongoSQLException.ErrorSQL(s);
	   }	
	
	public  void setParams(List params)
	   {
	     this._pos = 1;
	     this._params = params;
	   }
	   
	public DBCursor query() throws MongoSQLException{
        if (!(statement instanceof SQLSelectStatement)) {
        	throw new IllegalArgumentException("not a query sql statement");
        }
        DBCursor c=null;
        SQLSelectStatement selectStmt = (SQLSelectStatement)statement;
        SQLSelectQuery sqlSelectQuery =selectStmt.getSelect().getQuery();		
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			
			BasicDBObject fields = new BasicDBObject();
			//显示的字段
			for(SQLSelectItem item : mysqlSelectQuery.getSelectList()) {
				//System.out.println(item.toString());
				if (!(item.getExpr() instanceof SQLAllColumnExpr)) {
					fields.put(getFieldName(item), Integer.valueOf(1));
				}
			}	
			
			//表名
			SQLTableSource table=mysqlSelectQuery.getFrom();
			DBCollection coll =this._db.getCollection(table.toString());
			
			SQLExpr expr=mysqlSelectQuery.getWhere();	
			DBObject query = parserWhere(expr);
			//System.out.println(query);
			
			int limitoff=0;
			int limitnum=0;
			if (mysqlSelectQuery.getLimit()!=null) {
			  limitoff=getSQLExprToInt(mysqlSelectQuery.getLimit().getOffset());			
			  limitnum=getSQLExprToInt(mysqlSelectQuery.getLimit().getRowCount());
			}			
			if ((limitoff>0) || (limitnum>0)) {
			  c = coll.find(query, fields).skip(limitoff).limit(limitnum);
			}
			else {
			  c = coll.find(query, fields);
			}
			
			SQLOrderBy orderby=mysqlSelectQuery.getOrderBy();
			if (orderby != null ){
				BasicDBObject order = new BasicDBObject();
			    for (int i = 0; i < orderby.getItems().size(); i++)
			    {
			    	SQLSelectOrderByItem orderitem = orderby.getItems().get(i);
			       order.put(orderitem.getExpr().toString(), Integer.valueOf(getSQLExprToAsc(orderitem.getType())));
			    }
			    c.sort(order); 
			   // System.out.println(order);
			}			
			return c;
		}
		return c;		
	}
	
	public int executeUpdate() throws MongoSQLException {
        if (statement instanceof SQLInsertStatement) {
        	return InsertData((SQLInsertStatement)statement);
        }	
        if (statement instanceof SQLUpdateStatement) {
        	return UpData((SQLUpdateStatement)statement);
        }
        if (statement instanceof SQLDropTableStatement) {
        	return dropTable((SQLDropTableStatement)statement);
        }
        if (statement instanceof SQLDeleteStatement) {
        	return DeleteDate((SQLDeleteStatement)statement);
        }        
		return 1;
		
	}
	private int InsertData(SQLInsertStatement state) {
		if (state.getValues().getValues().size() ==0 ){
			throw new RuntimeException("number of  columns error");
		}		
		if (state.getValues().getValues().size() != state.getColumns().size()){
			throw new RuntimeException("number of values and columns have to match");
		}
		SQLTableSource table=state.getTableSource();
		BasicDBObject o = new BasicDBObject();
		int i=0;
		for(SQLExpr col : state.getColumns()) {
			o.put(getFieldName2(col), getExpValue(state.getValues().getValues().get(i)));
			i++;
		}		
		DBCollection coll =this._db.getCollection(table.toString());
		coll.insert(new DBObject[] { o });
		return 1;
	}
	private int UpData(SQLUpdateStatement state) {
		SQLTableSource table=state.getTableSource();
		DBCollection coll =this._db.getCollection(table.toString());
		
		SQLExpr expr=state.getWhere();
		DBObject query = parserWhere(expr);
		
		BasicDBObject set = new BasicDBObject();
		for(SQLUpdateSetItem col : state.getItems()){
			set.put(getFieldName2(col.getColumn()), getExpValue(col.getValue()));	
		}
		DBObject mod = new BasicDBObject("$set", set);
		coll.updateMulti(query, mod);
		//System.out.println("changs count:"+coll.getStats().size());
		return 1;		
	}
	private int DeleteDate(SQLDeleteStatement state) {
		SQLTableSource table=state.getTableSource();
		DBCollection coll =this._db.getCollection(table.toString());
		
		SQLExpr expr=state.getWhere();
		if (expr==null) {
			throw new RuntimeException("not where of sql");
		}
		DBObject query = parserWhere(expr);
		
		coll.remove(query);
		
		return 1;
		
	}
	private int dropTable(SQLDropTableStatement state) {		
		for (SQLTableSource table : state.getTableSources()){
			DBCollection coll =this._db.getCollection(table.toString());
			coll.drop();
		}
		return 1;
		
	}
	
	private int getSQLExprToInt(SQLExpr expr){
		if (expr instanceof SQLIntegerExpr){
			return ((SQLIntegerExpr)expr).getNumber().intValue();
		}
		return 0;		
	}
	private int getSQLExprToAsc(SQLOrderingSpecification ASC){
		if (ASC==null ) return 1;
		if (ASC==SQLOrderingSpecification.DESC){
			return -1;
		}
		else {
			return 1;		
		}
	}	
	public String remove(String resource,char ch)   
    {   
        StringBuffer buffer=new StringBuffer();   
        int position=0;   
        char currentChar;   
  
        while(position<resource.length())   
        {   
            currentChar=resource.charAt(position++);   
            if(currentChar!=ch) buffer.append(currentChar); 
        } 
        return buffer.toString();   
    }  
	private Object getExpValue(SQLExpr expr){
		if (expr instanceof SQLIntegerExpr){
			return ((SQLIntegerExpr)expr).getNumber().intValue();
		}
		if (expr instanceof SQLNumberExpr){
			return ((SQLNumberExpr)expr).getNumber();
		}		
		if (expr instanceof SQLCharExpr){
			String va=((SQLCharExpr)expr).toString();
			return remove(va,'\'');
		}
		if (expr instanceof SQLBooleanExpr){			
			return ((SQLBooleanExpr)expr).getValue();
		}			
		if (expr instanceof SQLNullExpr){
			return null;
		}
	    if (expr instanceof SQLVariantRefExpr) {
	       return this._params.get(this._pos++);
	    }		
		return expr;
		
	}
	private String getFieldName2(SQLExpr item){
		return item.toString();
	}
	
	private String getFieldName(SQLSelectItem item){
		return item.toString();
	}	
	private DBObject parserWhere(SQLExpr expr){
	    BasicDBObject o = new BasicDBObject();
	    parserWhere(expr,o);
	    return o;
	}
	
	
	
	private void parserDBObject(BasicDBObject ob,String akey, String aop,Object aval){
		boolean isok=false;
		if (!(ob.keySet().isEmpty())) {
          for (String field : ob.keySet()) {            
            if (akey.equals(field)){
               Object val = ob.get(field);	
              if (val instanceof BasicDBObject) {
            	 ((BasicDBObject) val).put(aop, aval);
            	 ob.put(field, (BasicDBObject) val); 
            	 isok=true;
            	 break;
              } else if (val instanceof BasicDBList) {
              //   newobj.put(field, ((BasicDBList)val).copy());
               }
            }  
          }    
        }    
		if (isok==false) {
			BasicDBObject xo = new BasicDBObject();
			xo.put(aop, aval);
			ob.put(akey,xo);	
		}
	    
	}
	
	@SuppressWarnings("unused")
	private void opSQLExpr(SQLBinaryOpExpr expr,BasicDBObject o) {
		   SQLExpr exprL=expr.getLeft();
		   if (!(exprL instanceof SQLBinaryOpExpr))
		   {
			  if (expr.getOperator().getName().equals("=")) {  
		        o.put(exprL.toString(), getExpValue(expr.getRight()));
			  }
			  else {
				  //BasicDBObject xo = new BasicDBObject();
				  String op="";
				  if (expr.getOperator().getName().equals("<"))   op="$lt";
				  if (expr.getOperator().getName().equals("<="))  op="$lte";
				  if (expr.getOperator().getName().equals(">"))   op="$gt";
				  if (expr.getOperator().getName().equals(">="))  op="$gte";
				  
				  if (expr.getOperator().getName().equals("!="))  op="$ne";
				  if (expr.getOperator().getName().equals("<>"))  op="$ne";
				  //xo.put(op, getExpValue(expr.getRight()));
				 // o.put(exprL.toString(),xo);
				  parserDBObject(o,exprL.toString(),op, getExpValue(expr.getRight()));
			  }
		   }		
	}
	private void parserWhere(SQLExpr aexpr,BasicDBObject o){   
	     if(aexpr instanceof SQLBinaryOpExpr){
		   SQLBinaryOpExpr expr=(SQLBinaryOpExpr)aexpr;  
		   SQLExpr exprL=expr.getLeft();
		   if (!(exprL instanceof SQLBinaryOpExpr))
		   {
			   //opSQLExpr((SQLBinaryOpExpr)aexpr,o);			   
			  if (expr.getOperator().getName().equals("=")) {  
		        o.put(exprL.toString(), getExpValue(expr.getRight()));
			  }
			  else {
				  String op="";
				  if (expr.getOperator().getName().equals("<"))   op="$lt";
				  if (expr.getOperator().getName().equals("<="))  op="$lte";
				  if (expr.getOperator().getName().equals(">"))   op="$gt";
				  if (expr.getOperator().getName().equals(">="))  op="$gte";
				  
				  if (expr.getOperator().getName().equals("!="))  op="$ne";
				  if (expr.getOperator().getName().equals("<>"))  op="$ne";

				  parserDBObject(o,exprL.toString(),op, getExpValue(expr.getRight()));
			  }
			  
		   }
		   else {
			 if (expr.getOperator().getName().equals("AND")) {  
			   parserWhere(exprL,o); 
			   parserWhere(expr.getRight(),o);
			 }
			 else if (expr.getOperator().getName().equals("OR")) {  
				orWhere(exprL,expr.getRight(),o); 				
			 }
			 else {
				 throw new RuntimeException("Can't identify the operation of  of where"); 
			 }
		   }
	   }
	  
	}
	
   
   private void orWhere(SQLExpr exprL,SQLExpr exprR ,BasicDBObject ob){ 
	   BasicDBObject xo = new BasicDBObject(); 
	   BasicDBObject yo = new BasicDBObject(); 
	   parserWhere(exprL,xo);
	   parserWhere(exprR,yo);
	   ob.put("$or",new Object[]{xo,yo});
   }	
}
