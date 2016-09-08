package io.mycat.net.mysql;

import io.mycat.backend.mysql.MySQLMessage;

/**
 * <pre>
 * 
 * COM_STMT_RESET resets the data of a prepared statement which was accumulated with COM_STMT_SEND_LONG_DATA commands and closes the cursor if it was opened with COM_STMT_EXECUTE

 * The server will send a OK_Packet if the statement could be reset, a ERR_Packet if not.
 * 
 * COM_STMT_RESET:
 * COM_STMT_RESET
 * direction: client -> server
 * response: OK or ERR

 * payload:
 *   1              [1a] COM_STMT_RESET
 *   4              statement-id
 * 
 * </pre>
 * 
 * @author CrazyPig
 * @since 2016-09-08
 *
 */
public class ResetPacket extends MySQLPacket {

	private static final byte PACKET_FALG = (byte) 26;
	private long pstmtId;
	
	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		byte code = mm.read();
		assert code == PACKET_FALG;
		pstmtId = mm.readUB4();
	}
	
	@Override
	public int calcPacketSize() {
		return 1 + 4;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Reset Packet";
	}

	public long getPstmtId() {
		return pstmtId;
	}

}
