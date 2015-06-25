package io.mycat.net2.mysql;

import java.util.List;

/**
 * mysql sql result status. for example ,query header result query rowset resut
 * ,ok result,
 * 
 * @author wuzhih
 * 
 */
public class ResultStatus {
	public static final int RESULT_STATUS_INIT = 0;
	public static final int RESULT_STATUS_HEADER = 1;
	public static final int RESULT_STATUS_FIELD_EOF = 2;

	private int resultStatus;
	private byte[] header;
	private List<byte[]> fields;

	public int getResultStatus() {
		return resultStatus;
	}

	public void setResultStatus(int resultStatus) {
		this.resultStatus = resultStatus;
	}

	public byte[] getHeader() {
		return header;
	}

	public void setHeader(byte[] header) {
		this.header = header;
	}

	public List<byte[]> getFields() {
		return fields;
	}

	public void setFields(List<byte[]> fields) {
		this.fields = fields;
	}

}
