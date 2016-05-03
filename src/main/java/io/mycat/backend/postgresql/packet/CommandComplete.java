package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.utils.PIOUtils;

//		CommandComplete (B)
//		Byte1('C')
//		标识此消息是一个命令结束响应。
//		
//		Int32
//		以字节记的消息内容的长度，包括长度本身。
//		
//		String
//		命令标记。它通常是一个单字，标识那个命令完成。
//		
//		对于INSERT命令，标记是INSERT oidrows， 这里的rows是插入的行数。oid 在row为 1 并且目标表有 OID 的时候是插入行的对象 ID； 否则oid就是 0。
//		
//		对于DELETE 命令，标记是 DELETE rows， 这里的 rows 是删除的行数。
//		
//		对于 UPDATE 命令，标记是 UPDATE rows 这里的 rows 是更新的行数。
//		
//		对于 MOVE 命令，标记是 MOVE rows，这里的 rows 是游标未知改变的行数。
//		
//		对于 FETCH 命令，标记是 FETCH rows，这里的 rows 是从游标中检索出来的行数。
public class CommandComplete extends PostgreSQLPacket {

	private int length;

	/**
	 * 命令
	 */
	private String commandResponse;

	@Override
	public int getLength() {
		return length;
	}

	public boolean isDDLComplete() {
		return commandResponse != null && (commandResponse.startsWith("INSERT") || commandResponse.startsWith("DELETE")
				|| commandResponse.startsWith("UPDATE"));
	}
	
	public boolean isTranComplete(){
		return commandResponse != null && (commandResponse.startsWith("ROLLBACK") || commandResponse.startsWith("COMMIT"));
	}

	public boolean isSelectComplete() {
		return commandResponse != null && (commandResponse.startsWith("SELECT"));
	}
	
	public int getRows() {
		if(!isDDLComplete()){
			return 0;
		}
		if (commandResponse != null) {
			String[] s = commandResponse.split(" +");
			if (s.length == 0) {
				return 0;
			}
			try {
				return Integer.valueOf(s[s.length - 1].trim());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return 0;
	}

	@Override
	public char getMarker() {
		return PacketMarker.B_CommandComplete.getValue();
	}

	public static CommandComplete parse(ByteBuffer buffer, int offset) {
		if (buffer.get(offset) != PacketMarker.B_CommandComplete.getValue()) {
			throw new IllegalArgumentException("this packetData not is CommandComplete");
		}
		CommandComplete packet = new CommandComplete();
		packet.length = PIOUtils.redInteger4(buffer, offset + 1);
		packet.commandResponse = new String(PIOUtils.redByteArray(buffer, offset + 1 + 4, packet.length - 4), UTF8);
		return packet;

	}

	public String getCommandResponse() {
		return commandResponse;
	}

	
}
