package io.mycat.sqlengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public interface SQLJobHandler {
    Logger LOGGER = LoggerFactory.getLogger(SQLJobHandler.class);

    void onHeader(String dataNode, byte[] header, List<byte[]> fields);

    boolean onRowData(String dataNode, byte[] rowData);

    void finished(String dataNode, boolean failed, String errorMsg);
}
