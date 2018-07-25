package io.mycat.sqlengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * SQL任务处理
 */
public interface SQLJobHandler {
	public static final Logger LOGGER = LoggerFactory.getLogger(SQLJobHandler.class);

	/**
	 * 头信息回调
	 * @param dataNode
	 * @param header
	 * @param fields
	 */
	public void onHeader(String dataNode, byte[] header, List<byte[]> fields);

	/**
	 * 记录数据回调
	 * @param dataNode
	 * @param rowData
	 * @return
	 */
	public boolean onRowData(String dataNode, byte[] rowData);

	/**
	 * 结束回调
	 * @param dataNode
	 * @param failed
	 * @param errorMsg
	 */
	public void finished(String dataNode, boolean failed, String errorMsg);
}
