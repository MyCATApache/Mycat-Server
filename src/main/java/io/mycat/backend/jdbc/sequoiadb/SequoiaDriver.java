package io.mycat.backend.jdbc.sequoiadb;

import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**  
 * 功能详细描述
 * @author sohudo[http://blog.csdn.net/wind520]
 * @create 2014年12月19日 下午6:50:23 
 * @version 0.0.1
 */
public class SequoiaDriver implements Driver

{
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SequoiaDriver.class);
    static final String PREFIX = "sequoiadb://";
    private DriverPropertyInfoHelper propertyInfoHelper = new DriverPropertyInfoHelper();
    
	static{
		try{
			DriverManager.registerDriver(new SequoiaDriver());
		}catch (SQLException e){
		    LOGGER.error("initError",e);
		}
	}


	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (url == null) {
			return null;
		}
		
		if (!StringUtils.startsWithIgnoreCase(url, PREFIX)) {	
			return null;//throw new IllegalArgumentException("uri needs to start with " + PREFIX);//return null;
		}
		String uri=url;
        uri = uri.substring(PREFIX.length());

        String serverPart;
        String nsPart;
        String optionsPart;
        {
            int idx = uri.lastIndexOf("/");
            if (idx < 0) {
                if (uri.contains("?")) {
                    throw new IllegalArgumentException("URI contains options without trailing slash");
                }
                serverPart = uri;
                nsPart = null;
                optionsPart = "";
            } else {
                serverPart = uri.substring(0, idx);
                nsPart = uri.substring(idx + 1);

                idx = nsPart.indexOf("?");
                if (idx >= 0) {
                    optionsPart = nsPart.substring(idx + 1);
                    nsPart = nsPart.substring(0, idx);
                } else {
                    optionsPart = "";
                }

            }
        }		
		SequoiaConnection result = null;
		//System.out.print(info);
		try{
			result = new SequoiaConnection(serverPart, nsPart);
		}catch (Exception e){
			throw new SQLException("Unexpected exception: " + e.getMessage(), e);
		}
		
		return result;
	}
	

	
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if (!StringUtils.startsWithIgnoreCase(url, PREFIX)) {
			return false;
		}
		return true;
	}
	
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {

		return propertyInfoHelper.getPropertyInfo();
	}	
	

	@Override
	public int getMajorVersion() {
		return 1;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return true;
	}
	@Override  
	public Logger getParentLogger() throws SQLFeatureNotSupportedException{
		return null;
	}

}
