package io.mycat.server.config.cluster;

import io.mycat.server.config.node.SystemConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalClusterSync implements ClusterSync{

	private final static Logger LOGGER = LoggerFactory.getLogger(LocalClusterSync.class);

	@Override
	public boolean switchDataSource(String dataHost, int curIndex) {
		File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
		FileOutputStream fileOut = null;
		try {
			Properties dnIndexProperties = new Properties();
			dnIndexProperties.load(new FileInputStream(file));
			String oldIndex = dnIndexProperties.getProperty(dataHost);
			String newIndex = String.valueOf(curIndex);
			if (newIndex.equals(oldIndex)) {
				return true;
			}
			dnIndexProperties.setProperty(dataHost, newIndex);
			LOGGER.info("save DataHost index  " + dataHost + " cur index " + curIndex);

			File parent = file.getParentFile();
			if (parent != null && !parent.exists()) {
				parent.mkdirs();
			}

			fileOut = new FileOutputStream(file);
			dnIndexProperties.store(fileOut, "update");
		} catch (Exception e) {
			LOGGER.warn("saveDataNodeIndex err:", e);
		} finally {
			if (fileOut != null) {
				try {
					fileOut.close();
				} catch (IOException e) {
				}
			}
		}
		return true;
	}

	@Override
	public void init() {


	}

}
