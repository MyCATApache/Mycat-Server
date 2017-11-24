package nl.anchormen.sql4es.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import nl.anchormen.sql4es.model.Utils;

/**
 * Basic {@link Driver} implementation used to get {@link ESConnection}.
 * 
 * @author cversloot
 *
 */
public class ESDriver implements Driver{

	private static final Logger logger = LoggerFactory.getLogger(ESDriver.class.getName());
	
	/**
	 * Register this driver with the driver manager
	 */
	static{
		try {
			DriverManager.registerDriver(new ESDriver());
		} catch (SQLException sqle) {
			logger.error("Unable to register Driver", sqle);
		}
	}
	
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		Object[] conInfo = parseURL(url, info);
		String host = (String)conInfo[0];
		int port = (int)conInfo[1];
		String index = (String)conInfo[2];
		Properties props = (Properties)conInfo[3];
		return new ESConnection(host, port, index, props);
	}
	/**
	 * Parses the url and returns information required to create a connection. Properties
	 * in the url are added to the provided properties and returned in the object array
	 * @param url
	 * @param info
	 * @return {String:host, int:port, String:index, Properties:info}
	 * @throws SQLException
	 */
	private Object[] parseURL(String url, Properties info) throws SQLException{
		if(!acceptsURL(url)) throw new SQLException("Invalid url");
		try {
			URI uri = new URI(url.substring(12));
			String host = uri.getHost();
			int port = (uri.getPort() < 0 ? Utils.PORT : uri.getPort());
			String index = uri.getPath().length() <= 1 ? null : uri.getPath().split("/")[1];
			Properties props = Utils.defaultProps();
			if(info != null) {
				props.putAll(info);
			}
			info = props;
			if(uri.getQuery() != null) 
				for(String keyValue : uri.getQuery().split("&")){
					String[] parts = keyValue.split("=");
					if(parts.length > 1) info.setProperty(parts[0].trim(), parts[1].trim());
					else info.setProperty(parts[0], "");
				}
			return new Object[]{host, port, index, info};
		} catch (URISyntaxException e) {
			throw new SQLException("Unable to parse URL. Pleas use '"+Utils.PREFIX+"//host:port/schema?{0,1}(param=value&)*'", e);
		}catch(ArrayIndexOutOfBoundsException e){
			throw new SQLException("No shema (index) specified. Pleas use '"+Utils.PREFIX+"//host:port/schema?{0,1}(param=value&)*'");
		}catch(Exception e){
			throw new SQLException("Unable to connect to database due to: "+e.getClass().getName(), e);
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if(!url.startsWith(Utils.PREFIX)) return false;
		try {
			URI uri = new URI(url.substring(5));
			if(uri.getHost() == null) throw new SQLException("Invalid URL, no host specified");
			if(uri.getPath() == null) throw new SQLException("Invalid URL, no index specified");
			if(uri.getPath().split("/").length > 2) throw new SQLException("Invalid URL, "+uri.getPath()+" is not a valid index");
		} catch (URISyntaxException e) {
			throw new SQLException("Unable to parse URL", e);
		}
		return true;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		Properties props = (Properties)parseURL(url, info)[3];
		DriverPropertyInfo[] result = new DriverPropertyInfo[props.size()];
		int index = 0;
		for(Object key : props.keySet()){
			result[index] = new DriverPropertyInfo((String)key, props.get(key).toString());
			index++;
		}
		return result;
	}

	@Override
	public int getMajorVersion() {
		return Utils.ES_MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion() {
		return Utils.ES_MINOR_VERSION;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException(Utils.getLoggingInfo());
	}

}
