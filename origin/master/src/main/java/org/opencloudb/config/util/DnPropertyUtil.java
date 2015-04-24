package org.opencloudb.config.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.opencloudb.config.model.SystemConfig;

/**
 * 
 * @author yanglixue
 *
 */
public class DnPropertyUtil {

	private static final Logger LOGGER = Logger.getLogger("DnPropertyUtil");
	
	/**
	 * 加载dnindex.properties属性文件
	 * @return 属性文件
	 */
	public static Properties loadDnIndexProps() {
		Properties prop = new Properties();
		File file = new File(SystemConfig.getHomePath(), "conf"
				+ File.separator + "dnindex.properties");
		if (!file.exists()) {
			return prop;
		}
		FileInputStream filein = null;
		try {
			filein = new FileInputStream(file);
			prop.load(filein);
		} catch (Exception e) {
			LOGGER.warn("load DataNodeIndex err:" + e);
		} finally {
			if (filein != null) {
				try {
					filein.close();
				} catch (IOException e) {
				}
			}
		}
		return prop;
	}
	
}
