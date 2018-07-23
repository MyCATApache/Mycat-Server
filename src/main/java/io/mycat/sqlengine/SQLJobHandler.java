package io.mycat.sqlengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * SQL任务处理
 */
public interface SQLJobHandler {
	public static final Logger LOGGER = LoggerFactory.getLogger(SQLJobHandler.class);

	public void onHeader(String dataNode, byte[] header, List<byte[]> fields);

	public boolean onRowData(String dataNode, byte[] rowData);

	public void finished(String dataNode, boolean failed, String errorMsg);
}
