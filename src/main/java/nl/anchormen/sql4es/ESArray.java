package nl.anchormen.sql4es;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import nl.anchormen.sql4es.model.Column;
import nl.anchormen.sql4es.model.Heading;

/**
 * Implementation of {@link Array} used to hold array's with primitive types recieved from elasticsearch
 * 
 * @author cversloot
 *
 */
public class ESArray implements Array {

	private List<Object> array;
	
	public ESArray(List<Object> array){
		this.array = array;
	}
	
	@Override
	public String getBaseTypeName() throws SQLException {
		if(array != null && array.size() > 0) return array.get(0).getClass().getName();
		else return ResultSet.class.getName();
	}

	@Override
	public int getBaseType() throws SQLException {
		if(array != null && array.size() > 0) return Heading.getTypeIdForObject( array.get(0) );
		else return Types.JAVA_OBJECT;
	}

	private List<Object> subList(long index, int count){
		int offset = (int)Math.max(0, index);
		int toIndex = (int)Math.min(array.size(), offset+count);
		return array.subList(offset, toIndex);
	}
	
	@Override
	public Object getArray() throws SQLException {
		if(array.size() == 0) return new Object[0];
		return array.toArray(new Object[array.size()]);
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		Object[] result = new Object[array.size()];
		for(int i=0; i<array.size(); i++){
			Object o = array.get(i);
			if(map.containsKey(o.getClass().getName())) result[i] = map.get(o.getClass().getName()).cast(o);
		}
		return result;
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		List<Object> subList = this.subList(index, count);
		Object[] result = new Object[subList.size()];
		return subList.toArray(result);
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
		List<Object> subList = this.subList(index, count);
		Object[] result = new Object[subList.size()];
		for(int i=0; i<subList.size(); i++){
			Object o = subList.get(i);
			if(map.containsKey(o.getClass().getName())) result[i] = map.get(o.getClass().getName()).cast(o);
		}
		return result;
	}

	private ResultSet getResultSet(List<Object> list, Map<String, Class<?>> map) throws SQLException{
		Heading heading = new Heading();
		heading.add(new Column("index").setSqlType(Types.INTEGER));
		heading.add(new Column("value").setSqlType(this.getBaseType()));
		ESResultSet rs = new ESResultSet(heading, list.size(), 2);
		for(int i=0; i<list.size(); i++){
			List<Object> row = rs.getNewRow();
			Object value = list.get(i);
			row.set(0, i);
			if(map != null && map.containsKey(value.getClass().getName())){
				row.set(1, map.get(value.getClass().getName()).cast(value));
			}else {
				row.set(1, value);
			}
			rs.add(row);
		}
		return rs;
	}
	
	@Override
	public ResultSet getResultSet() throws SQLException {
		return getResultSet(array, null);
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
		return getResultSet(array, map);
	}

	@Override
	public ResultSet getResultSet(long index, int count) throws SQLException {
		return getResultSet(this.subList(index, count), null);
	}

	@Override
	public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
		return getResultSet(this.subList(index, count), map);
	}

	@Override
	public void free() throws SQLException {
		this.array.clear();
	}
	
	public String toString(){
		if(array.size() == 0) return "[]";
		StringBuilder sb = new StringBuilder("[");
		for(int i=0; i<array.size()-1; i++) sb.append(array.get(i)+", ");
		sb.append(array.get(array.size()-1)+"]");
		return sb.toString();
	}

}
