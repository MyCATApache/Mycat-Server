package io.mycat.net.mysql;

import io.mycat.backend.mysql.MySQLMessage;

/**
 * 
 * <pre>
 * 
 * COM_STMT_SEND_LONG_DATA sends the data for a column. Repeating to send it, appends the data to the parameter.
 * No response is sent back to the client.

 * COM_STMT_SEND_LONG_DATA:
 * COM_STMT_SEND_LONG_DATA
 * direction: client -> server
 * response: none

 * payload:
 *   1              [18] COM_STMT_SEND_LONG_DATA
 *   4              statement-id
 *   2              param-id
 *   n              data
 * 
 * </pre>
 * 
 * @see https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
 * 
 * @author CrazyPig
 * @since 2016-09-08
 *
 */
public class LongDataPacket extends MySQLPacket {

	private static final byte PACKET_FALG = (byte) 24;
	private long pstmtId;
	private long paramId;
	private byte[] longData = new byte[0];
	
	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		byte code = mm.read();
		assert code == PACKET_FALG;
		pstmtId = mm.readUB4();
		paramId = mm.readUB2();
		this.longData = mm.readBytes(packetLength - (1 + 4 + 2));
	}

	@Override
	public int calcPacketSize() {
		return 1 + 4 + 2 + this.longData.length;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Long Data Packet";
	}

	public long getPstmtId() {
		return pstmtId;
	}

	public long getParamId() {
		return paramId;
	}

	public byte[] getLongData() {
		return longData;
	}

	
}
