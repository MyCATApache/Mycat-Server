package io.mycat.backend.jdbc.mongodb;

import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
//import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
//import java.util.HashMap;
import java.util.Map;
import java.util.Set;
/**  
 * 功能详细描述
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2014年12月19日 下午6:50:23 
 * @version 0.0.1
 */
public class MongoResultSet implements ResultSet
{
	 private final DBCursor _cursor;
	 private DBObject _cur;
	 private int _row = 0;
	 private boolean _closed = false;
	 private String[] select;
	 private int[] fieldtype;
	 private String _schema;
	 private String _table;
	 //支持聚合,包括count,group by 
	 private boolean isSum=false;
	 //是group by
	 private boolean isGroupBy=false;
	 private long _sum=0;
	 private BasicDBList dblist;
	 
	public MongoResultSet(MongoData mongo,String schema) throws SQLException {
	    this._cursor = mongo.getCursor();
	    this._schema = schema;
	    this._table  = mongo.getTable();
	    this.isSum   = mongo.getCount()>0;
	    this._sum    = mongo.getCount();
	    this.isGroupBy= mongo.getType();
	    
	    if (this.isGroupBy) {
	    	dblist  = mongo.getGrouyBys();
	    	this.isSum =true;
	    }
	    if (this._cursor!=null) {
	      select = (String[]) _cursor.getKeysWanted().keySet().toArray(new String[0]);
	    
	      if ( this._cursor.hasNext()){
	        _cur= _cursor.next(); 
	        if (_cur!=null) {
	           if (select.length==0) {
	    	      SetFields(_cur.keySet());
	           }	    	   
	          _row=1;
	         }
	      }  
	   
	     if (select.length==0){
		   select =new String[]{"_id"};
		   SetFieldType(true);
	    }
	    else {
		    SetFieldType(false);
	    }
	  }
	  else{
		  SetFields(mongo.getFields().keySet());//new String[]{"COUNT(*)"};	
		  SetFieldType(mongo.getFields());
	    }
	}
	
	public void SetFields(Set<String> keySet) {		
		this.select = new String[keySet.size()];
		this.select = keySet.toArray(this.select);

	}
	public void SetFieldType(boolean isid) throws SQLException {
		if (isid) {
		  fieldtype= new int[Types.VARCHAR];
		}
		else {
			fieldtype = new int[this.select.length];
		}
		
		if (_cur!=null) {
		  for (int i=0;i<this.select.length;i++){
			Object ob=this.getObject(i+1);
			fieldtype[i]=MongoData.getObjectToType(ob);
		  }	
		}
	}	
	
	public void SetFieldType(HashMap<String,Integer> map) throws SQLException {
        fieldtype= new int[this.select.length];
		  for (int i=0;i<this.select.length;i++){
			String ob=map.get(select[i]).toString();
			fieldtype[i]=Integer.parseInt(ob);
		  }	

	}	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		
		return false;
	}

	@Override
	public boolean next() throws SQLException {
		
		if ( isSum){	
			if (isGroupBy){
				_row++;
				if (_row<=dblist.size()) {
				   return true;
				}
		    	else {
		    	  return false;				
		    	}
			}
			else {
			  if (_row==1) {
				  return false;
			  }
			  else {
				_row++;
			    return true;
			  }
		    }
		}
		else {
			if (! this._cursor.hasNext()) {
	    	   if (_row==1) {
	    		  _row++;	
	    		  return true;
	    	   }
	    	   else {
				   return false;
			   }
	        }
	        else {
	           if (_row!=1){
	    	     this._cur = this._cursor.next(); 
	           }  
	           _row++;
	           return true;
	        }
		}
	}

	@Override
	public void close() throws SQLException {
		
		this._closed = true;
	}
	
	public String getField(int columnIndex){
	   return select[columnIndex-1];	
	}
	
	@Override
	public boolean wasNull() throws SQLException {
		
		return false;
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		
		return getString(getField(columnIndex));
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		
		return getBoolean(getField(columnIndex));
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		
		return getByte(getField(columnIndex));
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		
		return getShort(getField(columnIndex));
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		
		return getInt(getField(columnIndex));
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		
		return getLong(getField(columnIndex));
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		
		return getFloat(getField(columnIndex));
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		
		return getDouble(getField(columnIndex));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale)
			throws SQLException {
		
		return getBigDecimal(getField(columnIndex),scale);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		
		return getBytes(getField(columnIndex));
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		
		return getDate(getField(columnIndex));
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		
		return getTime(getField(columnIndex));
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		
		return getTimestamp(getField(columnIndex));
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		
		return getAsciiStream(getField(columnIndex));
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		
		return getUnicodeStream(getField(columnIndex));
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		
		return getBinaryStream(getField(columnIndex));
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		
		Object x = getObject(columnLabel);
		if (x == null) {
			return null;
		}
		return x.toString();
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		
		//return false;
		Object x = getObject(columnLabel);
		if (x == null) {
			return false;
		}
		return ((Boolean)x).booleanValue();
	}
	
	public Number getNumber(String columnLabel)
	 {
	   Number x = (Number)this._cur.get(columnLabel);
	   if (x == null) {
		   return Integer.valueOf(0);
	   }
	   return x;
	 }
	 
	@Override
	public byte getByte(String columnLabel) throws SQLException {
		
		return getNumber(columnLabel).byteValue();
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		
		return getNumber(columnLabel).shortValue();
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		
		return getNumber(columnLabel).intValue();
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		
		return getNumber(columnLabel).longValue();
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		
		return getNumber(columnLabel).floatValue();
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		
		return getNumber(columnLabel).doubleValue();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale)
			throws SQLException {
		
		throw new UnsupportedOperationException();//return null;
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		
		return (byte[])getObject(columnLabel);
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		
		return (Date)getObject(columnLabel);
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		
		return (Time)getObject(columnLabel);
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		Object obj = getObject(columnLabel);
		if(obj instanceof java.util.Date){
			java.util.Date d= (java.util.Date) obj;
			return new Timestamp(d.getTime());
		}
		return (Timestamp)obj;//throw new UnsupportedOperationException();
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		
		throw new UnsupportedOperationException();//return null;
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		
		throw new UnsupportedOperationException();//return null;
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		
		throw new UnsupportedOperationException();//return null;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		
		
	}

	@Override
	public String getCursorName() throws SQLException {
		
		return this._cursor.toString();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		
		return new MongoResultSetMetaData(select,fieldtype,this._schema,this._table);
		/*
	 	if(_cur !=null){
	 		return new MongoResultSetMetaData(_cur.keySet(),this._schema);  
	     }
	 	 else{ 		
	 		return new MongoResultSetMetaData(select,this._schema); 
	     } 		
	     */
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		
		 if (columnIndex == 0){
			 if (isSum) {
				 return getObject(getField(1)); 
			 }
			 else {
				 return this._cur;
			 }
		 }
		else {
			 return getObject(getField(columnIndex));
		 }
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		
		if (isSum) {
		   if (isGroupBy){
			  Object ob=dblist.get(_row-1);
			  if (ob instanceof DBObject) {
				  return ((DBObject)ob).get(columnLabel);
			  }
			  else {
				  return "0";  
			  }
		   }
		   else{
			   return  this._sum;
		   }
		}
		else {
			return this._cur.get(columnLabel);
		}
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		
		return 0;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		
		return getCharacterStream(getField(columnIndex));
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		
		return getBigDecimal(getField(columnIndex));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isFirst() throws SQLException {
		
		return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		
		return false;
	}

	@Override
	public void beforeFirst() throws SQLException {
		
		
	}

	@Override
	public void afterLast() throws SQLException {
		
		
	}

	@Override
	public boolean first() throws SQLException {
		
		return false;
	}

	@Override
	public boolean last() throws SQLException {
		
		return false;
	}

	@Override
	public int getRow() throws SQLException {
		
		return this._cursor.count();
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		// 按相对行数（或正或负）移动光标。
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		
		if (direction == getFetchDirection()) {
			return;
		}
	}

	@Override
	public int getFetchDirection() throws SQLException {
		
		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		// 设置此 ResultSet 对象需要更多行时应该从数据库获取的行数。
		
	}

	@Override
	public int getFetchSize() throws SQLException {
		
		return 0;
	}

	@Override
	public int getType() throws SQLException {
		
		return 0;
	}

	@Override
	public int getConcurrency() throws SQLException {
		
		return 0;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		// 获取是否已更新当前行。
		return false;
	}

	@Override
	public boolean rowInserted() throws SQLException {
		// 获取当前行是否已有插入。
		return false;
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		//获取是否已删除某行。
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		
		
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		
		
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		
		
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		
		
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		
		
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		
		
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		
		
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		
		
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x)
			throws SQLException {
		
		
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		
		
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		
		
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		
		
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		
		
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x)
			throws SQLException {
		
		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		
		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length)
			throws SQLException {
		
		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length)
			throws SQLException {
		
		
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength)
			throws SQLException {
		
		
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		
		
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		
		
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x)
			throws SQLException {
		
		
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		
		
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		
		
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		
		
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		
		
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		
		
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		
		
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x)
			throws SQLException {
		
		
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		
		
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		
		
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		
		
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		
		
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x)
			throws SQLException {
		
		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		
		
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length)
			throws SQLException {
		
		
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			int length) throws SQLException {
		
		
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength)
			throws SQLException {
		
		
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		
		
	}

	@Override
	public void insertRow() throws SQLException {
		
		
	}

	@Override
	public void updateRow() throws SQLException {
		
		
	}

	@Override
	public void deleteRow() throws SQLException {
		
		
	}

	@Override
	public void refreshRow() throws SQLException {
		
		
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		
		
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		
		
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		
		
	}

	@Override
	public Statement getStatement() throws SQLException {
		
		return null;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map)
			throws SQLException {
		
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		
		return null;//getArray(_find(i));
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map)
			throws SQLException {
		
		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal)
			throws SQLException {
		
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal)
			throws SQLException {
		
		return null;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		
		
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		
		
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		
		
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		
		
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		
		
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		
		
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		
		
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		
		
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		
		
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		
		
	}

	@Override
	public int getHoldability() throws SQLException {
		
		return 0;
	}

	@Override
	public boolean isClosed() throws SQLException {
		
		return this._closed;
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		
		
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		
		
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		
		
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		
		
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		
		
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		
		
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		
		return null;
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		
		return null;
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		
		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		
		
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		
		
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		
		
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		
		
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		
		
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		
		
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		
		
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		
		
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		
		
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		
		
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		
		
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		
		
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		
		
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		
		
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		
		
	}

	@Override
	public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		
		
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		
		
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		
		
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		Object value = getObject(columnIndex);
		return (T) MongoEmbeddedObjectProcessor.valueMapper(getField(columnIndex), value, type);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type)
			throws SQLException {
		Object value = getObject(columnLabel);
		return (T) MongoEmbeddedObjectProcessor.valueMapper(columnLabel, value, type);
	}
	
	
}
