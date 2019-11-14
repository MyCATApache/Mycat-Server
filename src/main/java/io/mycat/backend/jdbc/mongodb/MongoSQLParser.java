package io.mycat.backend.jdbc.mongodb;



import java.sql.Types;
import java.util.List;


import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSQLParser.class);
	private   final DB _db;
//	private   final String _sql;
	private   final SQLStatement statement;	
	private   List _params;
	private   int _pos;	
	public MongoSQLParser(DB db, String sql)  throws MongoSQLException
	   {
	     this._db = db;
	 //    this._sql = sql;
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
	         LOGGER.error("MongoSQLParser.parserError", e);
	    }
	     throw new MongoSQLException.ErrorSQL(s);
	   }	
	
	public  void setParams(List params)
	   {
	     this._pos = 1;
	     this._params = params;
	   }
	   
	public MongoData query() throws MongoSQLException{
        if (!(statement instanceof SQLSelectStatement)) {
        	//return null;
        	throw new IllegalArgumentException("not a query sql statement");
        }
        MongoData mongo=new MongoData();
        DBCursor c=null;
        SQLSelectStatement selectStmt = (SQLSelectStatement)statement;
        SQLSelectQuery sqlSelectQuery =selectStmt.getSelect().getQuery();	
        int icount=0;
		if(sqlSelectQuery instanceof MySqlSelectQueryBlock) {
			MySqlSelectQueryBlock mysqlSelectQuery = (MySqlSelectQueryBlock)selectStmt.getSelect().getQuery();
			
			BasicDBObject fields = new BasicDBObject();
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
			DBObject query = parserWhere(expr);
			//System.out.println(query);
			SQLSelectGroupByClause groupby=mysqlSelectQuery.getGroupBy();
			BasicDBObject gbkey = new BasicDBObject();
			if (groupby!=null) {
			  for (SQLExpr gbexpr:groupby.getItems()){
				if (gbexpr instanceof SQLIdentifierExpr) {
					String name=((SQLIdentifierExpr) gbexpr).getName();
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
			
			if (icount==1) {
				mongo.setCount(coll.count(query));						
			}
			else if (icount==2){
				BasicDBObject initial = new BasicDBObject();
				initial.put("num", 0);
				String reduce="function (obj, prev) { "
						+"  prev.num++}";
				mongo.setGrouyBy(coll.group(gbkey, query, initial, reduce));			
			}
			else {
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
		   }
		   mongo.setCursor(c);
		}
		return  mongo;		
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
        if (statement instanceof SQLCreateTableStatement) {
        	return 1;
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
		BasicDBObject[] oList = new BasicDBObject[state.getValuesList().size()];
		int i = 0;
		for(SQLInsertStatement.ValuesClause values : state.getValuesList()){
			int j = 0;
			BasicDBObject o = new BasicDBObject();
			oList[i++] = o ;
			for(SQLExpr col : state.getColumns()) {
				o.put(getFieldName2(col), getExpValue(values.getValues().get(j++)));
			}
		}

		DBCollection coll =this._db.getCollection(table.toString());
		WriteResult result = coll.insert(oList);
		return i; // 这里result.getN 总是返回0 , 所以按插入数据量返回影响行数
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
		WriteResult result = coll.updateMulti(query, mod);
		//System.out.println("changs count:"+coll.getStats().size());
		return result.getN();
	}
	private int DeleteDate(SQLDeleteStatement state) {
		SQLTableSource table=state.getTableSource();
		DBCollection coll =this._db.getCollection(table.toString());
		
		SQLExpr expr=state.getWhere();
		if (expr==null) {
			throw new RuntimeException("not where of sql");
		}
		DBObject query = parserWhere(expr);

		WriteResult result = coll.remove(query);
		return result.getN();
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
				  //xo.put(op, getExpValue(expr.getRight()));
				 // o.put(exprL.toString(),xo);
				  parserDBObject(o,exprL.toString(),op, getExpValue(expr.getRight()));
			  }
		   }		
	}

	private void parserWhere(SQLExpr aexpr,BasicDBObject o){

		if(aexpr instanceof SQLBinaryOpExpr){
			SQLBinaryOpExpr expr=(SQLBinaryOpExpr)aexpr;
			//处理AND和OR
			if (expr.getOperator().getName().equals("AND")) {
				parserWhere(expr.getLeft(),o);
				parserWhere(expr.getRight(),o);
			}
			else if (expr.getOperator().getName().equals("OR")) {
				orWhere(expr.getLeft(),expr.getRight(),o);
			} else {
				SQLExpr exprL=expr.getLeft();
				if (!(exprL instanceof SQLBinaryOpExpr)){
					if (expr.getOperator().getName().equals("=")) {
						o.put(exprL.toString(), getExpValue(expr.getRight()));

					}else if(("like").equals(expr.getOperator().getName().toLowerCase())){
						//处理like以及正则转换
						String likeString="";
						try{
							likeString=("%"+String.valueOf(getExpValue(expr.getRight()))+"%")
									.replace("%%","")
									.replace("%","^");
						}catch (Exception e){
							throw new RuntimeException("like SQL error");
						}

						parserDBObject(o,exprL.toString(),"$regex", likeString);

					} else {
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
			}
		}else if(aexpr instanceof  SQLInListExpr){
			//处理IN和NOT IN
			SQLInListExpr expr= (SQLInListExpr)aexpr;
			List<SQLExpr> exprList= expr.getTargetList();
			BasicDBList dbObject= new BasicDBList();
			for(SQLExpr e:exprList){
				dbObject.add(getExpValue(e));
			}
			String op="$in";
			if(expr.isNot()){
				op="$nin";
			}
			parserDBObject(o,expr.getExpr().toString(),op, dbObject);
		}
	}


	private void parserWhereOLD(SQLExpr aexpr,BasicDBObject o){
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
	
   
   private void orWhere(SQLExpr exprL,SQLExpr exprR ,BasicDBObject ob){ 
	   BasicDBObject xo = new BasicDBObject(); 
	   BasicDBObject yo = new BasicDBObject(); 
	   parserWhere(exprL,xo);
	   parserWhere(exprR,yo);
	   ob.put("$or",new Object[]{xo,yo});
   }	
}
