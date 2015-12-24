package io.mycat.sqlengine;

import io.mycat.server.packet.FieldPacket;
import io.mycat.server.packet.RowDataPacket;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OneRawSQLQueryResultHandler implements SQLJobHandler {

	private Map<String, Integer> fetchColPosMap;
	private final SQLQueryResultListener<SQLQueryResult<Map<String, String>>> callback;
	private final String[] fetchCols;
	private int fieldCount = 0;
	private Map<String, String> result ;
	public OneRawSQLQueryResultHandler(String[] fetchCols,
			SQLQueryResultListener<SQLQueryResult<Map<String, String>>> callBack) {

		this.fetchCols = fetchCols;
		this.callback = callBack;
	}

	public void onHeader(String dataNode, byte[] header, List<byte[]> fields) {
		fieldCount = fields.size();
		fetchColPosMap = new HashMap<String, Integer>();
		for (String watchFd : fetchCols) {
			for (int i = 0; i < fieldCount; i++) {
				byte[] field = fields.get(i);
				FieldPacket fieldPkg = new FieldPacket();
				fieldPkg.read(field);
				String fieldName = new String(fieldPkg.name);
				if (watchFd.equalsIgnoreCase(fieldName)) {
					fetchColPosMap.put(fieldName, i);
				}
			}
		}

	}

	@Override
	public boolean onRowData(String dataNode, byte[] rowData) {
		RowDataPacket rowDataPkg = new RowDataPacket(fieldCount);
		rowDataPkg.read(rowData);
		result = new HashMap<String, String>();
		for (String fetchCol : fetchCols) {
			Integer ind = fetchColPosMap.get(fetchCol);
			if (ind != null) {
				byte[] columnData = rowDataPkg.fieldValues.get(ind);
                String columnVal = columnData!=null?new String(columnData):null;
                result.put(fetchCol, columnVal);

			} else {
				LOGGER.warn("cant't find column in sql query result "
						+ fetchCol);
			}
		}
        
		// 返回false，表示还有数据要处理，数据处理没有结束;
		// 如果返回true，连接会被SQLJob关闭：conn.close("not needed by user proc")
		// 对应的各种资源：socketchannel,read buffer,write buffer等都会被回收，连接会被从连接池中删除
		return false;	
	}

	@Override
	public void finished(String dataNode, boolean failed) {
		SQLQueryResult<Map<String, String>> queryResult=
				new SQLQueryResult<Map<String, String>>(this.result, !failed, dataNode);
	     this.callback.onResult(queryResult);

	}

	// 子类 MultiRowSQLQueryResultHandler 需要使用
	protected Map<String, String> getResult() {
		return result;
	}

}
