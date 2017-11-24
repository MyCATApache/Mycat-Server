package nl.anchormen.sql4es;

import java.sql.SQLException;
import java.util.List;

import nl.anchormen.sql4es.model.Heading;
import nl.anchormen.sql4es.model.QuerySource;

/**
 * Interface for objects holding query state information used during SQL parsing by presto
 * 
 * @author cversloot
 *
 */
public interface QueryState {

	/**
	 * Provides the original query provided to the driver
	 * @return
	 */
	public String originalSql();

	/**
	 * Returns the heading build for the query
	 * @return
	 */
	public Heading getHeading();
	
	/**
	 * Adds exception to this state signaling something went wrong while traversing the AST
	 * @param msg
	 */
	public void addException(String msg);
	
	/**
	 * 
	 * @return true if an exception has been set (typically a flag to stop work and return)
	 */
	public boolean hasException();
	
	/**
	 * @return the SQLExceptoin set in this state or NULL if it has no exception
	 */
	public SQLException getException();
	
	/**
	 * Gets the specified integer property
	 * @param name
	 * @param def
	 * @return
	 */
	public int getIntProp(String name, int def);
	
	/**
	 * Gets the specified string property
	 * @param name
	 * @param def
	 * @return
	 */
	public String getProperty(String name, String def);
	
	/**
	 * Gets the property with the given name or NULL if it does not exist
	 * @param name
	 * @return
	 */
	public Object getProperty(String name);

	/**
	 * Gets set of relations being accessed in the query (if any)
	 * @return
	 */
	public List<QuerySource> getSources();
	
	public QueryState setKeyValue(String key, Object value);
	
	public Object getValue(String key);
	
	public String getStringValue(String key);
	
}
