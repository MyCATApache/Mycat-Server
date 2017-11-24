package nl.anchormen.sql4es.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Utils {
	
	// statics
	public static final int ES_MAJOR_VERSION = 2;
	public static final int ES_MINOR_VERSION = 1;
	public static final String ELASTICSEARCH_NAME = "Elasticsearch";
	public static final String ELASTICSEARCH_VERSION = "2.1";
	public static final String CATALOG_SEPARATOR = ".";
	public static final int DRIVER_MAJOR_VERSION = 0;
	public static final int DRIVER_MINOR_VERSION = 5;
	
	// connection defaults
	public static final String PREFIX = "jdbc:sql4es:";
	public static final int PORT = 9300;
	
	// defaults
	private static final int FETCH_SIZE = 10000; // 10K is current max for ES
	private static final int SCROLL_TIMEOUT_SEC = 10;
	private static final int QUERY_TIMEOUT_MS = 10000;
	private static final int DEFAULT_ROW_LENGTH = 250; // used during initialization of rows when querying all columns (Select *)
	private static final String QUERY_CACHE = "query_cache";
	private static final String RESULT_NESTED_LATERAL = "true";
	private static final int FRAGMENT_SIZE = 100;
	private static final int FRAGMENT_NUMBER = 1;
	
	// property keys
	public static final String PROP_FETCH_SIZE = "fetch.size";
	public static final String PROP_SCROLL_TIMEOUT_SEC = "scroll.timeout.sec";
	public static final String PROP_QUERY_TIMEOUT_MS = "query.timeout.ms";
	public static final String PROP_DEFAULT_ROW_LENGTH = "default.row.length";
	public static final String PROP_QUERY_CACHE_TABLE = "query.cache.table";
	public static final String PROP_RESULT_NESTED_LATERAL = "result.nested.lateral";
	public static final String PROP_TABLE_COLUMN_MAP = "table.column.info.map";
	public static final String PROP_FRAGMENT_SIZE = "fragment.size";
	public static final String PROP_FRAGMENT_NUMBER = "fragment.number";
	public static final String PROP_USER = "user";
	public static final String PROP_PASSWORD = "password";
	public static String getLoggingInfo(){
		StackTraceElement element = Thread.currentThread().getStackTrace()[2];
		return element.getClassName()+"."+element.getMethodName()+" ["+element.getLineNumber()+"]";
	}
	
	public static List<Object> clone(List<Object> list){
		List<Object> copy = new ArrayList<Object>(list.size());
		for(Object o : list) copy.add(o);
		return copy;
	}

	public static Properties defaultProps(){
		Properties defaults = new Properties();
		defaults.put(PROP_FETCH_SIZE, FETCH_SIZE);
		defaults.put(PROP_SCROLL_TIMEOUT_SEC, SCROLL_TIMEOUT_SEC);
		defaults.put(PROP_DEFAULT_ROW_LENGTH, DEFAULT_ROW_LENGTH);
		defaults.put(PROP_QUERY_CACHE_TABLE, QUERY_CACHE);
		defaults.put(PROP_QUERY_TIMEOUT_MS, QUERY_TIMEOUT_MS);
		defaults.put(PROP_RESULT_NESTED_LATERAL, RESULT_NESTED_LATERAL);
		defaults.put(PROP_FRAGMENT_SIZE, FRAGMENT_SIZE);
		defaults.put(PROP_FRAGMENT_NUMBER, FRAGMENT_NUMBER);
		return defaults;
	}
	
	/**
	 * Retrieves the integer property with given name from the properties
	 * @param props
	 * @param name
	 * @param def
	 * @return
	 */
	public static int getIntProp(Properties props, String name, int def){
		if(!props.containsKey(name)) return def;
		try {
			return Integer.parseInt(props.getProperty(name));
		} catch (Exception e) {
			return def;
		}
		
	}

	/**
	 * Retrieves the integer property with given name from the properties
	 * @param props
	 * @param name
	 * @param def
	 * @return
	 */
	public static boolean getBooleanProp(Properties props, String name, boolean def){
		if(!props.containsKey(name)) return def;
		try {
			return Boolean.parseBoolean( props.getProperty(name) );
		} catch (Exception e) {
			return def;
		}
		
	}

	public static Object getObjectProperty(Properties props, String name) {
		return props.get(name);
	}
	
	public static void sleep(int millis) {
		try{
			Thread.sleep(millis);
		}catch(Exception e){}
	}
}
