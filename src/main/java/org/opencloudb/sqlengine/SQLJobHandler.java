package org.opencloudb.sqlengine;

import java.util.List;

public interface SQLJobHandler {

	public void onHeader(String dataNode, byte[] header, List<byte[]> fields);

	public boolean onRowData(String dataNode, byte[] rowData);

	public void finished(String dataNode, boolean failed);
}
