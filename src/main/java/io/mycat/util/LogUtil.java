package io.mycat.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Locale;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.model.SystemConfig;

public class LogUtil {
	public static final Logger LOGGER = LoggerFactory.getLogger(LogUtil.class);

	/**
	 *  將消息寫入到logs\switch.log中
	 *
	 * @param
	 * @param msg
	 */
	public synchronized static void writeDataSourceLog(String msg) {
		File file = new File(SystemConfig.getHomePath(), "logs" + File.separator + "switch.log");
		FileOutputStream fileOut = null;
		try {
			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				parent.mkdirs();
			}
			fileOut = new FileOutputStream(file, true);
			long time = TimeUtil.currentTimeMillis();
	    	DateTime dt2 = new DateTime(time);
			String m = String.format("%s: %s\r\n",dt2.toString(DateUtil.DEFAULT_DATE_PATTERN,Locale.CHINESE) ,
					msg);
			fileOut.write(m.getBytes());;
			
		} catch (Exception e) {
			LOGGER.warn("write dataHost log err:", e);
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
}
