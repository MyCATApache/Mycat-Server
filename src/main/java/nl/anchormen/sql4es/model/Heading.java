package nl.anchormen.sql4es.model;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.anchormen.sql4es.model.Column.Operation;

/**
 * Represents all the columns and their column index in the {@link ResultSet}. In addition it keeps track
 * of special scenarios such as 'select * from ..' in which all available columns should be returned or 
 * when client side calculations must be done like sum(column)/10. To support these calculations a Heading might
 * have invisible columns used to store data not accessible to the client.   
 * 
 * @author cversloot
 *
 */
public class Heading {

	public final static String ID = "_id";
	public final static String INDEX = "_index";
	public final static String TYPE = "_type";
	public static final String SCORE = "_score";
	public static final String SEARCH = "_search";
	
	private List<Column> columns = new ArrayList<Column>();
	private HashMap<String, Column> fieldIndex = new HashMap<String, Column>();
	private HashMap<String, Column> aliasIndex = new HashMap<String, Column>();
	private HashMap<String, Integer> labelToColNr = new HashMap<String, Integer>();
	private HashMap<Integer, Integer> columnToColIndex = new HashMap<Integer, Integer>();
	private Map<String, Integer> typeIndex = new HashMap<String, Integer>();
	private boolean allColumns = false;
	private boolean indexed = false;
	
	public Heading(){}
	
	/**
	 * Adds a column to this heading, checking if the column exists as well as adding its type.
	 * @param column
	 */
	public void add(Column column) {
		String colName = column.getColumn(); 
		// remove .* at the end to convert 'field.*' just to 'field' which will be expanded during result parsing
		if(colName.endsWith(".*")) column.setColumn(colName.substring(0, colName.length()-2));
		if(column.getColumn().equals("*") && column.getOp() == Operation.NONE) {
			this.allColumns  = true;
			String table = column.getTable();
			String tableAlias = column.getTableAlias();
			this.add(new Column(ID).setSqlType(Types.VARCHAR).setIndex(getColumnCount()).setTable(table, tableAlias));
			this.add(new Column(INDEX).setSqlType(Types.VARCHAR).setIndex(getColumnCount()).setTable(table, tableAlias));
			this.add(new Column(TYPE).setSqlType(Types.VARCHAR).setIndex(getColumnCount()).setTable(table, tableAlias));
			
			for(String field : this.typeIndex.keySet()){
				if(field.equals(ID) || field.equals(TYPE) || field.equals(INDEX)) continue;
				this.add(new Column(field).setSqlType(typeIndex.get(field)).setTable(table, tableAlias));
			}
		} else {
			column.setIndex(this.getColumnCount());
			columns.add(column);
			fieldIndex.put(column.getColumn(), column);
			if(column.getAlias() != null) aliasIndex.put(column.getAlias(), column);
		}
		// set type
		if(column.getColumn().equals(SCORE)) column.setSqlType(Types.DOUBLE);
		if(column.getOp() == Operation.COUNT) column.setSqlType(Types.BIGINT);
		else if(column.getOp() == Operation.AVG || column.hasCalculation()) column.setSqlType(Types.FLOAT);
		else if(column.getSqlType() != Types.ARRAY && typeIndex.containsKey(column.getColumn())){
			column.setSqlType(typeIndex.get(column.getColumn()));
		}
	}
	
	public void remove(Column column){
		for(int i=0; i<columns.size(); i++){
			Column col = columns.get(i);
			if(col.getIndex() == column.getIndex()){
				columns.remove(i);
				this.buildIndex();
				break;
			}
		}
	}
	
	/**
	 * Returns if any of the columns equals '*' indicating all fields must be
	 * fetched and provided in the ResultSet
	 * @return
	 */
	public boolean hasAllCols(){
		return allColumns;
	}
	
	public int getColumnCount(){
		return columns.size();
	}
	
	public Iterable<Column> columns(){
		return columns;
	}
	
	public Column getColumn(int index){
		return columns.get(index);
	}
	
	public Column getColumnByAlias(String alias){
		return aliasIndex.get(alias);
	}
	
	public boolean hasAlias(String alias){
		return aliasIndex.containsKey(alias);
	}
	
	public Column getColumnByLabel(String label){
		return fieldIndex.get(label);
	}
	
	public Column getColumnByNameAndOp(String colName, Operation op){
		for(Column col : columns){ // first check columnname and operation
			if(col.getColumn().equals(colName) && op == col.getOp()) return col;
		}
		for(Column col : columns){ // else check if it matches an alias
			if(colName.equals(col.getAlias())) return col;
		}
		return null;
	}
	
	public boolean hasLabel(String label){
		return fieldIndex.containsKey(label);
	}
	
	public boolean hasLabelStartingWith(String prefix) {
		for(String label : fieldIndex.keySet()){
			if(label.startsWith(prefix) && getColumnByLabel(label).getOp() != Operation.HIGHLIGHT ) return true;
		}
		return false;
	}

	public Column getFirstColumnStartingWith(String prefix) {
		for(String label : fieldIndex.keySet()){
			if(label.startsWith(prefix)) return fieldIndex.get(label);
		}
		return null;
	}
	
	public int getJDBCColumnNr(String label) throws SQLException{
		if(!indexed){
			buildIndex();
			indexed = true;
		}
		if(labelToColNr.containsKey(label)){
			return labelToColNr.get(label);
		}
		throw new SQLException("Column with label '"+label+"' does not exist");
	}

	public void setTypes(Map<String, Integer> types){
		if(types == null) return;
		this.typeIndex = types;
		// update types of any columns present within this heading
		for(Column s : columns){
			if(typeIndex.containsKey(s.getColumn())){
				s.setSqlType(typeIndex.get(s.getColumn()));
			}
		}
	}
	
	public Heading setAllColls(boolean value) {
		this.allColumns = value;
		return this;
	}
	
	public boolean followNode(String key){
		return allColumns || this.hasLabel(key) || this.hasLabelStartingWith(key);
	}
	
	/**
	 * Builds the indexes used by the heading for fast access of values in rows. Indexes include
	 * from column name, alias, and jdbc column number (starting at 1) to right array-index 
	 */
	public void buildIndex(){
		fieldIndex.clear();
		labelToColNr.clear();
		aliasIndex.clear();
		columnToColIndex.clear();
		int visIndex = 1;
		for(Column col : columns) {
			fieldIndex.put(col.getLabel(), col);
			fieldIndex.put(col.getAggName(), col);
			labelToColNr.put(col.getLabel(), visIndex);
			labelToColNr.put(col.getFullName(), visIndex);
			if(col.getAlias() != null) aliasIndex.put(col.getAlias(), col);
			if(col.isVisible()){
				columnToColIndex.put(visIndex, col.getIndex());
				visIndex++;
			}
		}
	}
	
	/**
	 * This function fixes column setup after all columns have been provided. It does 3 things:
	 * <ol>
	 * <li>Finds and converts lower-cased column labels and aliases back into their original</li>
	 * <li>Orders the columns as they were specified within the sql (might be mixed up if calculations are present)</li>
	 * <li>Rebuilds the mapping from name/alias to column index</li>
	 * </ol>
	 * @param originalSql
	 * @param columns
	 */
	public void reorderAndFixColumns(String originalSql, String prefix, String suffix){
		//Heading.fixColumnReferences(originalSql, prefix, suffix, columns);
		Collections.sort(this.columns);
		for(int i=0; i<columns.size(); i++) columns.get(i).setIndex(i);
		buildIndex();
	}
	
	public static void fixColumnReferences(String originalSql, String prefix, String suffix, List<Column> columns){
		for(Column c : columns){
			String name = c.getColumn().replaceAll("\\*", "\\\\*");
			String original = findOriginal(originalSql, name, prefix, suffix);
			c.setColumn(original);
			if(c.getOp() != Operation.NONE && c.getAlias() == null){
				switch(c.getOp()){
					case AVG: name = "AVG\\(\\s*"+name+"\\s*\\)"; break;
					case COUNT: name = "COUNT\\(\\s*"+name+"\\s*\\)";break;
					case MAX: name = "MAX\\(\\s*"+name+"\\s*\\)"; break;
					case MIN: name = "MIN\\(\\s*"+name+"\\s*\\)"; break;
					case SUM: name = "SUM\\(\\s*"+name+"\\s*\\)"; break;
					case HIGHLIGHT: name = "HIGHLIGHT\\(\\s*"+name+"\\s*\\)"; break;
					default: name = c.getColumn();
				}
				String alias = findOriginal(originalSql, name, prefix, suffix);
				c.setAlias(alias);
			}
		}
	}
	
	public static String findOriginal(String originalSql, String target, String prefix, String suffix){
		String pattern = target.replaceAll("\\*","\\\\*").replaceAll("\\(", "\\\\s*\\\\(\\\\s*")
				.replaceAll("\\)", "\\\\s*\\\\)\\\\s*").replaceAll("\\$", "\\\\\\$");
		//Matcher.quoteReplacement(target); // does not work for all cases it seems
		Pattern p = Pattern.compile(prefix+"("+pattern+")"+suffix, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(originalSql.replaceAll("\"", "")); // replace quotes to fix things like SELECT "table"."field" FROM ...
		if(m.find()){
			return m.group(1);
		}
		return target;
	}

	/**
	 * Returns if this heading specifies just a count() in which case there is no need to
	 * fetch all the data
	 * @return
	 */
	public boolean aggregateOnly() {
		if(this.hasAllCols()) return false;
		for(Column s : columns) if(s.getOp() == Operation.NONE && s.getCalculation() == null) return false;
		return true;
	}
	
	/**
	 * Gets the real index for the provided column number (starting with 1)
	 * @param nr
	 * @return
	 */
	public Integer getIndexForColumn(int nr) throws SQLException{
		if(!indexed || columnToColIndex.size() == 0){
			buildIndex();
			indexed = true;
		}
		Integer idx = columnToColIndex.get(nr);
		if(idx == null) throw new SQLException("Column "+nr+" does not exist");
		return idx;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(" allvisible = "+allColumns+" [");
		for(Column col : columns) sb.append(col.getLabel() +" ("+col.isVisible()+"), ");
		sb.append("]");
		return sb.toString();
	}
	
	/**
	 * Returns the class associated with a java.sql.Types id
	 * @param type
	 * @return
	 */
	public static Class<?> getClassForTypeId(int type){
		switch(type){
			case Types.ARRAY : return Array.class;
			case Types.BIGINT : return Long.class;
			case Types.TINYINT : return Byte.class;
			case Types.BINARY : return Byte[].class;
			case Types.BIT : return Boolean.class;
			case Types.BOOLEAN : return Boolean.class;
			case Types.CHAR : return Character.class;
			case Types.DATE : return java.sql.Date.class;
			case Types.DOUBLE : return Double.class;
			case Types.FLOAT : return Float.class;
			case Types.INTEGER : return Integer.class;
			case Types.NUMERIC : return BigDecimal.class;
			case Types.SMALLINT : return Short.class;
			case Types.LONGVARCHAR : return String.class;
			case Types.REAL : return Float.class;
			case Types.VARCHAR : return String.class;
			case Types.TIME : return Time.class;
			case Types.TIMESTAMP : return Timestamp.class;
			case Types.LONGVARBINARY : return Byte[].class;
			case Types.VARBINARY : return Byte[].class;
			default : return Object.class;
		}
	}

	public static int getTypeIdForObject(Object c) {
		if (c instanceof Long)
			return Types.BIGINT;
		if (c instanceof Boolean)
			return Types.BOOLEAN;
		if (c instanceof Character)
			return Types.CHAR;
		if (c instanceof java.sql.Date)
			return Types.DATE;
		if (c instanceof java.util.Date)
			return Types.DATE;
		if (c instanceof Double)
			return Types.DOUBLE;
		if (c instanceof Integer)
			return Types.INTEGER;
		if (c instanceof BigDecimal)
			return Types.NUMERIC;
		if (c instanceof Short)
			return Types.SMALLINT;
		if (c instanceof Float)
			return Types.FLOAT;
		if (c instanceof String)
			return Types.VARCHAR;
		if (c instanceof Time)
			return Types.TIME;
		if (c instanceof Timestamp)
			return Types.TIMESTAMP;
		if (c instanceof Byte)
			return Types.TINYINT;
		if (c instanceof Byte[])
			return Types.VARBINARY;
		if(c instanceof Object[])
			return Types.JAVA_OBJECT;
		if(c instanceof Object)
			return Types.JAVA_OBJECT;
		if (c instanceof Array)
			return Types.ARRAY;
		else
			return Types.OTHER;
	}

}
