package org.opencloudb.sqlengine;

import java.util.List;

import org.apache.log4j.Logger;

public interface SQLJobHandler {
	public static final Logger LOGGER = Logger.getLogger(SQLJobHandler.class);

	public void onHeader(String dataNode, byte[] header, List<byte[]> fields);

	public boolean onRowData(String dataNode, byte[] rowData);

	public void finished(String dataNode, boolean failed);
}
