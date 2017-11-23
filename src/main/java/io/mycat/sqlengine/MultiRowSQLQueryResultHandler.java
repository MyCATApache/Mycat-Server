package io.mycat.sqlengine;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 当SQLJob的结果有多行时，利用该处理器进行处理
 * @author digdeep@126.com
 */
public class MultiRowSQLQueryResultHandler extends OneRawSQLQueryResultHandler{
	private static final Logger LOGGER = LoggerFactory
				.getLogger(MultiRowSQLQueryResultHandler.class);
	// 获得结果之后，利用该对象进行回调进行通知和处理结果
	private final SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> callback;
	
	private List<Map<String, String>> resultRows = new LinkedList<>();	// 保存结果行
	
	public MultiRowSQLQueryResultHandler(String[] fetchCols,
			SQLQueryResultListener<SQLQueryResult<List<Map<String, String>>>> callback) {
		super(fetchCols, null);
		this.callback = callback;
	}

	@Override
	public boolean onRowData(String dataNode, byte[] rowData) {
		super.onRowData(dataNode, rowData);
		resultRows.add(getResult());
		/*
		* 重新创建一个result对象，否则得到的结果每一行的数据都一样<p>
		* 如果不是每次进入该方法时重新创建一个Result。那么此处获取到的result始终是同一个map，最终resultRows的所有结果都是相同的键值对（OneRawSQLQueryResultHandler会重复替换该值），因此我们需要重新创建result
		 */
		result = new HashMap<String, String>();
		return false;
	}

	@Override
	public void finished(String dataNode, boolean failed, String errorMsg) {
		SQLQueryResult<List<Map<String, String>>> queryResult = 
				new SQLQueryResult<List<Map<String, String>>>(this.resultRows, !failed);
		queryResult.setErrMsg(errorMsg);
		if(callback != null)
			this.callback.onResult(queryResult); // callback 是构造函数传进来，在得到结果是进行回调
		else
			LOGGER.warn(" callback is null ");
	}


}
