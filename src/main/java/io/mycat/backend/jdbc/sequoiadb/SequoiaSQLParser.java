package io.mycat.backend.jdbc.sequoiadb;



import java.sql.Types;
import java.util.List;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

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
public class SequoiaSQLParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(SequoiaSQLParser.class);
	private   final CollectionSpace _db;
//	private   final String _sql;
	private   final SQLStatement statement;	
	private   List _params;
	private   int _pos;	
	public SequoiaSQLParser(CollectionSpace db, String sql)  throws SequoiaSQLException
	   {
	     this._db = db;
	 //    this._sql = sql;
	     this.statement = parser(sql);
	   }
	
	public SQLStatement parser(String s) throws SequoiaSQLException
	   {
	     s = s.trim();
	     try
	     {
	        MySqlStatementParser parser = new MySqlStatementParser(s);
	        return parser.parseStatement();
	     }
	     catch (Exception e)
	     {
	         LOGGER.error("MongoSQLParser.parserError", e);
	    }
	     throw new SequoiaSQLException.ErrorSQL(s);
	   }	
	
	public  void setParams(List params)
	   {
	     this._pos = 1;
	     this._params = params;
	   }
	   
	public SequoiaData query() throws SequoiaSQLException{
        if (!(statement instanceof SQLSelectStatement)) {
        	//return null;
        	throw new IllegalArgumentException("not a query sql statement");
        }
        SequoiaData mongo=new SequoiaData();
        DBCursor c=null;
        SQLSelectStatement selectStmt = (SQLSelectStatement)statement;
        SQLSelectQuery sqlSelectQuery =selectStmt.getSelect().getQuery();	
        int icount=0;
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			
			BasicBSONObject fields = new BasicBSONObject();
			//显示的字段
			for(SQLSelectItem item : mysqlSelectQuery.getSelectList()) {
				//System.out.println(item.toString());
				if (!(item.getExpr() instanceof SQLAllColumnExpr)) {
					if (item.getExpr() instanceof SQLAggregateExpr) {
						SQLAggregateExpr expr =(SQLAggregateExpr)item.getExpr();
						if (expr.getMethodName().equals("COUNT")) {
						   icount=1;
						   mongo.setField(getExprFieldName(expr), Types.BIGINT);
						}
						fields.put(getExprFieldName(expr), Integer.valueOf(1));
					}
					else {					
					   fields.put(getFieldName(item), Integer.valueOf(1));
					}
				}
				
			}	
			
			//表名
			SQLTableSource table=mysqlSelectQuery.getFrom();
			DBCollection coll =this._db.getCollection(table.toString());
			mongo.setTable(table.toString());
			
			SQLExpr expr=mysqlSelectQuery.getWhere();	
			BSONObject query = parserWhere(expr);
			//System.out.println(query);
			SQLSelectGroupByClause groupby=mysqlSelectQuery.getGroupBy();
			BasicBSONObject gbkey = new BasicBSONObject();
			if (groupby!=null) {
			  for (SQLExpr gbexpr:groupby.getItems()){
				if (gbexpr instanceof SQLIdentifierExpr) {
					String name =((SQLIdentifierExpr) gbexpr).getName();
					gbkey.put(name, Integer.valueOf(1));
				}
			  }
			  icount=2;
			}	
			int limitoff=0;
			int limitnum=0;
			if (mysqlSelectQuery.getLimit()!=null) {
			  limitoff=getSQLExprToInt(mysqlSelectQuery.getLimit().getOffset());			
			  limitnum=getSQLExprToInt(mysqlSelectQuery.getLimit().getRowCount());
			}	
			
			  SQLOrderBy orderby=mysqlSelectQuery.getOrderBy();
			  BasicBSONObject order = new BasicBSONObject();
			  if (orderby != null ){				
			    for (int i = 0; i < orderby.getItems().size(); i++)
			    {
			    	SQLSelectOrderByItem orderitem = orderby.getItems().get(i);
			       order.put(orderitem.getExpr().toString(), Integer.valueOf(getSQLExprToAsc(orderitem.getType())));
			    }
			  //  c.sort(order); 
			   // System.out.println(order);
			  }
			  
			if (icount==1) {
				mongo.setCount(coll.getCount(query));						
			}
			else if (icount==2){
				BasicBSONObject initial = new BasicBSONObject();
				initial.put("num", 0);
				String reduce="function (obj, prev) { "
						+"  prev.num++}";
				//mongo.setGrouyBy(coll.group(gbkey, query, initial, reduce));			
			}
			else {
			  if ((limitoff>0) || (limitnum>0)) {
			    c = coll.query(query, fields, order,null, limitoff, limitnum);//.skip(limitoff).limit(limitnum);
			  }
			  else {
			   c = coll.query(query, fields, order,null, 0, -1);
			  }
			

		   }
		   mongo.setCursor(c);
		}
		return  mongo;		
	}
	
	public int executeUpdate() throws SequoiaSQLException {
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
        if (statement instanceof SQLCreateTableStatement) {
        	return createTable((SQLCreateTableStatement)statement);
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
		BSONObject o = new BasicBSONObject();
		int i=0;
		for(SQLExpr col : state.getColumns()) {
			o.put(getFieldName2(col), getExpValue(state.getValues().getValues().get(i)));
			i++;
		}		
		DBCollection coll =this._db.getCollection(table.toString());
		//coll.insert(new DBObject[] { o });
		coll.insert(o);
		return 1;
	}
	private int UpData(SQLUpdateStatement state) {
		SQLTableSource table=state.getTableSource();
		DBCollection coll =this._db.getCollection(table.toString());
		
		SQLExpr expr=state.getWhere();
		BSONObject query = parserWhere(expr);
		
		BasicBSONObject set = new BasicBSONObject();
		for(SQLUpdateSetItem col : state.getItems()){
			set.put(getFieldName2(col.getColumn()), getExpValue(col.getValue()));	
		}
		BSONObject mod = new BasicBSONObject("$set", set);
		//coll.updateMulti(query, mod);
		coll.update(query, mod, null);
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
		BSONObject query = parserWhere(expr);
		
		//coll.remove(query);
		coll.delete(query);
		return 1;
		
	}
	private int dropTable(SQLDropTableStatement state) {		
		for (SQLTableSource table : state.getTableSources()){
			//DBCollection coll =this._db.getCollection(table.toString());
			//coll.drop();
			this._db.dropCollection(table.toString());
		}
		return 1;
		
	}
	
	private int createTable(SQLCreateTableStatement state) {		
		//for (SQLTableSource table : state.getTableSource()){
			if (!this._db.isCollectionExist(state.getTableSource().toString())) {
				this._db.createCollection(state.getTableSource().toString());
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
		if (ASC==null ) {
			return 1;
		}
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
            if(currentChar!=ch) {
				buffer.append(currentChar);
			}
        } 
        return buffer.toString();   
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
	private String getExprFieldName(SQLAggregateExpr expr){
		String field="";
		for (SQLExpr item :expr.getArguments()){
			field+=item.toString();
		}		
		return expr.getMethodName()+"("+field+")";
		
	}
	private String getFieldName2(SQLExpr item){
		return item.toString();
	}
	
	private String getFieldName(SQLSelectItem item){
		return item.toString();
	}	
	private BSONObject parserWhere(SQLExpr expr){
	    BasicBSONObject o = new BasicBSONObject();
	    parserWhere(expr,o);
	    return o;
	}
	
	
	
	private void parserDBObject(BasicBSONObject ob,String akey, String aop,Object aval){
		boolean isok=false;
		if (!(ob.keySet().isEmpty())) {
          for (String field : ob.keySet()) {            
            if (akey.equals(field)){
               Object val = ob.get(field);	
              if (val instanceof BasicBSONObject) {
            	 ((BasicBSONObject) val).put(aop, aval);
            	 ob.put(field, (BasicBSONObject) val); 
            	 isok=true;
            	 break;
              } else if (val instanceof BasicBSONList) {
              //   newobj.put(field, ((BasicDBList)val).copy());
               }
            }  
          }    
        }    
		if (isok==false) {
			BasicBSONObject xo = new BasicBSONObject();
			xo.put(aop, aval);
			ob.put(akey,xo);	
		}
	    
	}
	
	@SuppressWarnings("unused")
	private void opSQLExpr(SQLBinaryOpExpr expr,BasicBSONObject o) {
		   SQLExpr exprL=expr.getLeft();
		   if (!(exprL instanceof SQLBinaryOpExpr))
		   {
			  if (expr.getOperator().getName().equals("=")) {  
		        o.put(exprL.toString(), getExpValue(expr.getRight()));
			  }
			  else {
				  //BasicBSONObject xo = new BasicBSONObject();
				  String op="";
				  if (expr.getOperator().getName().equals("<")) {
					  op="$lt";
				  }
				  if (expr.getOperator().getName().equals("<=")) {
					  op = "$lte";
				  }
				  if (expr.getOperator().getName().equals(">")) {
					  op = "$gt";
				  }
				  if (expr.getOperator().getName().equals(">=")) {
					  op = "$gte";
				  }
				  if (expr.getOperator().getName().equals("!=")) {
					  op = "$ne";
				  }
				  if (expr.getOperator().getName().equals("<>")) {
					  op = "$ne";
				  }
				  //xo.put(op, getExpValue(expr.getRight()));
				 // o.put(exprL.toString(),xo);
				  parserDBObject(o,exprL.toString(),op, getExpValue(expr.getRight()));
			  }
		   }		
	}
	private void parserWhere(SQLExpr aexpr,BasicBSONObject o){   
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
				  if (expr.getOperator().getName().equals("<")) {
					  op = "$lt";
				  }
				  if (expr.getOperator().getName().equals("<=")) {
					  op = "$lte";
				  }
				  if (expr.getOperator().getName().equals(">")) {
					  op = "$gt";
				  }
				  if (expr.getOperator().getName().equals(">=")) {
					  op = "$gte";
				  }
				  if (expr.getOperator().getName().equals("!=")) {
					  op = "$ne";
				  }
				  if (expr.getOperator().getName().equals("<>")) {
					  op = "$ne";
				  }

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
	
   
   private void orWhere(SQLExpr exprL,SQLExpr exprR ,BasicBSONObject ob){ 
	   BasicBSONObject xo = new BasicBSONObject(); 
	   BasicBSONObject yo = new BasicBSONObject(); 
	   parserWhere(exprL,xo);
	   parserWhere(exprR,yo);
	   ob.put("$or",new Object[]{xo,yo});
   }	
}
