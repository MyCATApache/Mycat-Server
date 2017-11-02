package nl.anchormen.sql4es;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.OrderBy;
import nl.anchormen.sql4es.model.Utils;
import nl.anchormen.sql4es.model.expression.IComparison;

public class ESResultSet implements ResultSet {

	private List<List<Object>> rows = new ArrayList<List<Object>>();
	private Heading heading;
	private int cursor = -1;
	private ESQueryState req;
	private long total;
	private long offset = 0;
	private int defaultRowLength = 1000;

	public ESResultSet(ESQueryState req){
		this.heading = req.getHeading();
		this.req = req;
		this.total = 0;
		this.defaultRowLength = req.getIntProp(Utils.PROP_DEFAULT_ROW_LENGTH, 1000);
	}
	
	public ESResultSet(Heading heading, int total, int defaultRowLength){
		this.heading = heading;
		this.total = total;
		this.defaultRowLength = defaultRowLength;
	}
	
	public ESResultSet(ESQueryState req, long total) {
		this.heading = req.getHeading();
		this.req = req;
		this.total = total;
		this.defaultRowLength = req.getIntProp(Utils.PROP_DEFAULT_ROW_LENGTH, 1000);
	}
	
	public ESResultSet(ESQueryState req, long offset, long total){
		this(req, total);
		this.offset = offset;
	}
	
	public Heading getHeading(){
		return this.heading;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(Column h : heading.columns()){
			if(h.isVisible() ) sb.append(h.getLabel()+", ");
		}
		sb.append("\r\n");
		for(List<Object> row : rows){
			for(Column h : heading.columns()){
				Object o = h.getIndex() >= row.size() ? null : row.get(h.getIndex());
				if(h.isVisible() ) sb.append((o instanceof ResultSet ? "\r\n" : "")+o+(o instanceof ResultSet ? "\r\n" : ", "));
			}
			sb.append("\r\n");
		}
		return sb.toString().trim();
	}
	
	/**
	 * Creates a new row for this resultset with proper initial capacity (if known) and initialized with NULL's. 
	 * The row still needs to be added to the resultset!
	 * @return
	 */
	public List<Object> getNewRow(){
		List<Object> row = Arrays.asList(new Object[defaultRowLength]);
		return row;
	}
	
	public void add(List<Object> row) {
		rows.add(row.subList(0, heading.getColumnCount()));
		if(rows.size() > total) total = rows.size(); // can happen when rows are being exploded
	}
	
	public int rowCount(){
		return rows.size();
	}
	
	public List<Object> getRow(int index){
		return this.rows.get(index);
	}
	
	public void orderBy(List<OrderBy> order){
		Collections.sort(rows, new ResultRowComparator(order));
	}
	
	public int getNrRows(){
		return rows.size();
	}
	
	public void setTotal(int total){
		this.total = total;
	}
	
	public long getTotal(){
		return total;
	}
	
	public long getOffset(){
		return this.offset;
	}
	
	public void limit(int limit){
		if(rows.size() > limit) rows = rows.subList(0, limit);
	}
	
	/**
	 * Removes rows that do not match the provided Having clause
	 * @param having
	 * @throws SQLException
	 */
	public void filterHaving(IComparison having) throws SQLException{
		for(int i=0; i<rows.size(); i++){
			if(!having.evaluate(rows.get(i))){
				rows.remove(i);
				i--;
			}
		}
		this.total = rows.size();
	}
	
	/**
	 * Executes any computations specified on columns
	 */
	public void executeComputations(){
		boolean calculationFound = false;
		for(Column column : heading.columns()) 
			if(column.hasCalculation()){
				calculationFound = true;
				break;
		}
		if(!calculationFound) return;
		for(int i=0; i<rows.size(); i++){
			for(Column column : heading.columns()){
				if(column.hasCalculation()) {
					Number value = column.getCalculation().evaluate(this, i);
					rows.get(i).set(column.getIndex(), value);
				}
			}
		}
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean next() throws SQLException {
		if(cursor + 1 < rows.size() && offset + cursor + 1 < total){
			cursor ++;
			return true;
		}
		return false;
	}

	@Override
	public void close() throws SQLException {	}

	@Override
	public boolean wasNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	private Object getForColumn(int columnIdx) throws SQLException{
		Integer idx = heading.getIndexForColumn(columnIdx);
		if(idx >= rows.get(cursor).size()) return null;
		return rows.get(cursor).get(idx);
	}
	
	@Override
	public String getString(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return (String)value;
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type String but is "+value.getClass());
		}
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return false;
		try{
			int type = heading.getColumn(heading.getIndexForColumn(columnIndex)).getSqlType();
			switch(type){
				case Types.BIGINT : return ((Long)value).longValue() == 1; 
				case Types.INTEGER : return ((Integer)value).intValue() == 1; 
				case Types.DOUBLE : return ((Double)value).doubleValue() == 1; 
				case Types.FLOAT : return ((Float)value).floatValue() == 1; 
				case Types.BIT : return (Boolean)value; 
				case Types.BOOLEAN : return (Boolean)value; 
				case Types.CHAR : return Boolean.parseBoolean(""+((char)value));
				case Types.VARCHAR : return Boolean.parseBoolean((String)value);
				default : return false;
			}
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type boolean but is "+value.getClass());
		}
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return 0;
		try{
			return (byte)value;
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type byte but is "+value.getClass());
		}
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return 0;
		try{
			return ((Number)value).shortValue();
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type short but is "+value.getClass());
		}
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return 0;
		try{
			return  ((Number)value).intValue();
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type int but is "+value.getClass());
		}
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return 0;
		try{
			return  ((Number)value).longValue();
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type Long but is "+value.getClass());
		}
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return 0;
		try{
			return  ((Number)value).floatValue();
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type Float but is "+value.getClass());
		}
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return 0;
		try{
			return  ((Number)value).doubleValue();
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type Double but is "+value.getClass());
		}
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return (BigDecimal)value;
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type BigDecimal but is "+value.getClass());
		}
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return (byte[])value;
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type Byte[] but is "+value.getClass());
		}
	}
	
	private long getTimeFromString(String value) throws SQLException{
		try{
			return DateTime.parse(value).getMillis();
		}catch(Exception e){
			throw new SQLException("Unable to parse Date from '"+value+"' : "+e.getMessage());
		}
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		int type = heading.getColumn(heading.getIndexForColumn(columnIndex)).getSqlType();
		if(type == Types.DATE){
			if(value instanceof String) return new Date(getTimeFromString((String)value));
			return new Date(((java.util.Date)value).getTime());
		}else throw new SQLException("Value in column '"+columnIndex+"' is not a Date but is "+value.getClass().getSimpleName());
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		int type = heading.getColumn(heading.getIndexForColumn(columnIndex)).getSqlType();
		if(type == Types.DATE){
			if(value instanceof String) return new Time(getTimeFromString((String)value));
			return new Time(((java.util.Date)value).getTime());
		}else throw new SQLException("Value in column '"+columnIndex+"' is not a Date but is "+value.getClass().getSimpleName());
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		int type = heading.getColumn(heading.getIndexForColumn(columnIndex)).getSqlType();
		if(type == Types.DATE){
			if(value instanceof String) return new Timestamp(getTimeFromString((String)value));
			return new Timestamp(((java.util.Date)value).getTime());
		}else throw new SQLException("Value in column '"+columnIndex+"' is not a date but is "+value.getClass().getSimpleName());
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		return getString(findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return getBoolean(findColumn(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return getByte(findColumn(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return getShort(findColumn(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return getInt(findColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return getLong(findColumn(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return getFloat(findColumn(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return getDouble(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		return getBigDecimal(findColumn(columnLabel), scale);
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return getBytes(findColumn(columnLabel));
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return getDate(findColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return getTime(findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return getTimestamp(findColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO
	}

	@Override
	public String getCursorName() throws SQLException {
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		List<Column> visibleCols = new ArrayList<Column>();
		for(Column col : heading.columns())
			if(col.isVisible()) {
				visibleCols.add(col);
				/*
				if(col.getSqlType() == Types.OTHER){
					for(int i=0; i<Math.min(100, rows.size()); i++){
						Object value = rows.get(i).get(col.getIndex());
						if(value != null){
							col.setSqlType( Heading.getTypeIdForObject(value) );
							break;
						}
					}
				}*/
			}
		return new ESResultSetMetaData(visibleCols, "", "");
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		try{
			return getForColumn(columnIndex);
		}catch(Exception e){
			throw new SQLException("Unable for get value for column "+columnIndex);
		}
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return getObject(findColumn(columnLabel));
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException {
		// adjust real index of the column to the JDBC standard one (starting at 1 instead of 0)
		return heading.getJDBCColumnNr(columnLabel);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return new StringReader((String)value);
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type String but is "+value.getClass());
		}
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return getCharacterStream(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return (BigDecimal)value;
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type BigDecimal but is "+value.getClass());
		}
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return getBigDecimal(findColumn(columnLabel));
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return cursor < 0;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return cursor >= total;
	}

	@Override
	public boolean isFirst() throws SQLException {
		return cursor == 1 && rows.size() > 0;
	}

	@Override
	public boolean isLast() throws SQLException {
		return cursor == total-1 && cursor > -1;
	}

	@Override
	public void beforeFirst() throws SQLException {
		cursor = -1;
	}

	@Override
	public void afterLast() throws SQLException {
		cursor = (int)total;
	}

	@Override
	public boolean first() throws SQLException {
		cursor = 1;
		return isFirst();
	}

	@Override
	public boolean last() throws SQLException {
		cursor = rows.size() - 1;
		return true;
	}

	@Override
	public int getRow() throws SQLException {
		return cursor;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		if(row >=0 && row < rows.size()){
			cursor = row;
			return true;
		}
		return false;
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		int newRow = cursor + rows;
		if(newRow >=0 && newRow < this.rows.size()){
			cursor = newRow;
			return true;
		}
		return false;
	}

	@Override
	public boolean previous() throws SQLException {
		if(cursor - 1 > -1){
			cursor--;
			return true;
		}
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		// TODO
	}

	@Override
	public int getFetchDirection() throws SQLException {
		// TODO
		return 0;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException {
		return req.getMaxRows();
	}

	@Override
	public int getType() throws SQLException {
		return ResultSet.TYPE_SCROLL_INSENSITIVE;
	}

	@Override
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLFeatureNotSupportedException();	
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLFeatureNotSupportedException();	
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();	
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();		
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();	
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException();	
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException();	
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement getStatement() throws SQLException {
		return req.getStatement();
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		Object res = getForColumn(columnIndex);
		if(res == null) return null;
		if(map.containsKey(res.getClass().getName())) try {
			Class<?> clas = map.get(res.getClass().getName());
			return clas.cast(res);
		}catch(Exception e){
			throw new SQLException("Unable to fetch and cast object for column '"+columnIndex+"'", e);
		}
		return null;
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return (Array)value;
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type Array but is "+value.getClass());
		}
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		Object res = getObject(findColumn(columnLabel));
		if(res == null) return null;
		if(map.containsKey(res.getClass().getName())) try {
			Class<?> clas = map.get(res.getClass().getName());
			return clas.cast(res);
		}catch(Exception e){
			throw new SQLException("Unable to fetch and cast object for column '"+columnLabel+"'", e);
		}
		return null;
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		return getArray(findColumn(columnLabel));
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return new URL((String)value);
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type URL but is "+value.getClass());
		}
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		return getURL(findColumn(columnLabel));
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.req.getStatement().isClosed();
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return (String)value;
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type String but is "+value.getClass());
		}
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return getNString(findColumn(columnLabel));
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return new StringReader((String)value);
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type String but is "+value.getClass());
		}
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return getNCharacterStream(findColumn(columnLabel));
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();	
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		Object value = getForColumn(columnIndex);
		if(value == null) return null;
		try{
			return type.cast(value);
		}catch(Exception cce){
			throw new SQLException("Value in column '"+columnIndex+"' is not of type "+type+" but is "+value.getClass());
		}
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return getObject(findColumn(columnLabel), type);
	}

	/**
	 * Comparator implementation used to sort Rows in the resultSet based on a {@link OrderBy} List
	 * @author cversloot
	 *
	 */
	public class ResultRowComparator implements Comparator<List<Object>>{

		public List<OrderBy> order;
		
		public ResultRowComparator(List<OrderBy> order) {
			this.order = order;
		}
		
		@Override
		public int compare(List<Object> rr1, List<Object> rr2) {
			int res = 0;
			for(OrderBy ob : order) try{
				// order NULL values last
				Object o1 = rr1.get(ob.getIndex());
				if(o1 == null ) return ob.func() * 1;
				Object o2 = rr2.get(ob.getIndex());
				if(o2 == null )return ob.func() * -1;
				
				if(o1 instanceof String){
					res = ((String)o1).compareTo((String)o2); 
				}else if(o1 instanceof Number){
					res = new Double(((Number)o1).doubleValue()).compareTo(new Double(((Number)o2).doubleValue()));
				}
				if(res != 0) return ob.func() * res;
			}catch(Exception e){
				return 0;
			}
			return res;
		}
		
	}

	public ESResultSet setOffset(int offset) {
		this.offset = offset;
		return this;
	}

}
