package org.opencloudb.sharejoin;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
/**  
 * 功能详细描述:分片join,单独的语句
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2015年02月01日 
 * @version 0.0.1
 */

public class TableFilter {
	protected static final Logger LOGGER = Logger.getLogger(TableFilter.class);
	
	private LinkedHashMap<String,String> fieldAliasMap = new LinkedHashMap<String,String>();
	private String tName;
	private String tAlia;
	private String where="";
	private String order="";
	
	private String parenTable="";//左连接的join的表
    private String joinParentkey="";//左连接的join字段
    private String joinKey="";	//join字段
	
	private TableFilter join;
	private TableFilter parent;
	
	private int offset=0;
	private int rowCount=0;
	
	private boolean outJoin;
	private boolean allField;
	public TableFilter(String taName,String taAlia,boolean outJoin) {
		this.tName=taName;
		this.tAlia=taAlia;
		this.outJoin=outJoin;
		this.where="";
	}	
	
	private String getTablefrom(String key){
		if (key==null){
			return "";	
		}
		else {
			int i=key.indexOf('.');
			if (i==-1){
				return key;
			}
			else
			  return key.substring(0, i);
		}
		
	}
	private String getFieldfrom(String key){
		if (key==null){
			return "";	
		}
		else {
		  int i=key.indexOf('.');
			if (i==-1){
				return key;
			}
			else		  
		       return key.substring(i+1);
		}
	}
	
	public void addField(String fieldName,String fieldAlia){
		String atable=getTablefrom(fieldName);
		String afield=getFieldfrom(fieldName);
		boolean allfield=afield.equals("*")?true:false;
		if (atable.equals("*")) {
		  fieldAliasMap.put(afield, null);
		  setAllField(allfield);
		  if (join!=null) {
			 join.addField(fieldName,null);  
			 join.setAllField(allfield);
		   }		  
		}
		else {
		  if (atable.equals(tAlia)) {
		    fieldAliasMap.put(afield, fieldAlia);
		    setAllField(allfield);
		 }
		  else {
		    if (join!=null) {
			  join.addField(fieldName,fieldAlia);  
			  join.setAllField(allfield);
		     }
		   }
		}
	}
	
	public void addWhere(String fieldName,String value,String Operator,String and){
		String atable=getTablefrom(fieldName);
		String afield=getFieldfrom(fieldName);
		if (atable.equals(tAlia)) {
			where=unionsql(where,afield+Operator+value,and);
		}
		else {
		  if (join!=null) {
			  join.addWhere(fieldName,value,Operator,and);  
		  }
		}
	}
	
    private String unionsql(String key,String value,String Operator){
    	if (key.trim().equals("")){
    		key=value;
    	}
    	else {
    		key+=" "+Operator+" "+value;
    	}
    	return key;
    }
	
	public void addOrders(String fieldName,String des){
		String atable=getTablefrom(fieldName);
		String afield=getFieldfrom(fieldName);
		if (atable.equals(tAlia)) {
			order=unionsql(order,afield+" "+des,",");
		}
		else {
		  if (join!=null) {
			  join.addOrders(fieldName,des);  
		  }
		}
	}	
	public void addLimit(int offset,int rowCount){
		this.offset=offset;
		this.rowCount=rowCount;
	}
	public void setJoinKey(String fieldName,String value){
		if (parent==null){
			if (join!=null)	{
				join.setJoinKey(fieldName,value);
			}
		}
		else {
		 int i=joinLkey(fieldName,value);
		 if (i==1){
			 joinParentkey=getFieldfrom(value);
			 parenTable   =getTablefrom(value);
			 joinKey=getFieldfrom(fieldName);
		 }
		 else {			
		   if (i==2){
			   joinParentkey=getFieldfrom(fieldName);
			   parenTable   =getTablefrom(fieldName);
			   joinKey=getFieldfrom(value);
		   }
		   else {
				  if (join!=null) {
					  join.setJoinKey(fieldName,value);  
				  }		   
		   }
		 }
		}
	}
	
	private String getChildJoinKey(boolean left){
	   if (join!=null){
			if (left) {
				return join.joinParentkey;
			}
			else {
				return join.joinKey;
			}		   
	   }
	   else {
		   return "";
	   }
	}
	public String getJoinKey(boolean left){
		return getChildJoinKey(left);
	}
    private int joinLkey(String fieldName,String value){
    	String key1=getTablefrom(fieldName);
    	String key2=getTablefrom(value);    	
    	if (key1.equals(tAlia) ) {
    		return 1;
    	}
    	
    	if (key2.equals(tAlia) ) {
    		return 2;
    	}     	
    	/*
    	 String bAlia=""; 
    	if (join!=null){
    		bAlia=join.getTableAlia();
    	}
    	if (key1.equals(tAlia)&& key2.equals(bAlia) ) {
    		return 1;
    	}
    	
    	if (key2.equals(tAlia)&& key1.equals(bAlia) ) {
    		return 2;
    	} 
    	*/
    	return 0;
    }	
	
	public String getTableName(){
		return tName;
	}	
	public void setTableName(String value){
		tName=value;
	}
	
	public String getTableAlia(){
		return tAlia;
	}	
	public void setTableAlia(String value){
		tAlia=value;
	}	
	
	public boolean getOutJoin(){
		return outJoin;
	}	
	public void setOutJoin(boolean value){
		outJoin=value;
	}
	
	
	public boolean getAllField(){
		return allField;
	}	
	public void setAllField(boolean value){
		allField=value;
	}	
	
	public TableFilter getTableJoin(){
		return join;
	}	
	public void setTableJoin(TableFilter  value){
		join=value;
		join.setParent(this);
	}	
    public TableFilter getParent() {
        return parent;
    }

    public void setParent(TableFilter parent) {
        this.parent = parent;
    }	
    
    private String unionField(String field,String key,String Operator){
    	if (key.trim().equals("")){
    		key=field;
    	}
    	else {
    		key=field+Operator+" "+key;
    	}
    	return key;
    }
    
	public String getSQL(){
		String sql="";
		Iterator<Entry<String, String>> iter = fieldAliasMap.entrySet().iterator();
		while (iter.hasNext()) {
		  Map.Entry<String, String> entry = (Map.Entry<String, String>) iter.next();
		  String key = entry.getKey();
		  String val = entry.getValue();		
			if (val==null) {
			  sql=unionsql(sql,getFieldfrom(key),",");
			}
			else
			  sql=unionsql(sql,getFieldfrom(key)+" as "+val,",");  
		  }
        if (parent==null){	// on/where 等于号左边的表
        	String parentJoinKey = getJoinKey(true);
        	// fix sharejoin bug： 
        	// (AbstractConnection.java:458) -close connection,reason:program err:java.lang.IndexOutOfBoundsException:
        	// 原因是左表的select列没有包含 join 列，在获取结果时报上面的错误
        	if(sql != null && parentJoinKey != null &&  
        			sql.toUpperCase().indexOf(parentJoinKey.trim().toUpperCase()) == -1){
        		sql += ", " + parentJoinKey;
        	}
		   sql="select "+sql+" from "+tName;
		   if (!(where.trim().equals(""))){
				sql+=" where "+where.trim(); 	
			}
        }
        else {	// on/where 等于号右边边的表
        	if (allField) {
        	   sql="select "+sql+" from "+tName;
        	}
        	else {
        	   sql=unionField("select "+joinKey,sql,",");
        	   sql=sql+" from "+tName;		
        	   //sql="select "+joinKey+","+sql+" from "+tName;
        	}
    		if (!(where.trim().equals(""))){
    			sql+=" where "+where.trim()+" and ("+joinKey+" in %s )"; 	
    		}
    		else {
    			sql+=" where "+joinKey+" in %s "; 
    		}
        }        	

		if (!(order.trim().equals(""))){
			sql+=" order by "+order.trim(); 	
		}	
		if (parent==null){
        	if ((rowCount>0)&& (offset>0)){
        		sql+=" limit"+offset+","+rowCount;
        	}
        	else {
        		if (rowCount>0){
        			sql+=" limit "+rowCount;
        		}
        	}
		}	
		return sql; 
	}	
}
