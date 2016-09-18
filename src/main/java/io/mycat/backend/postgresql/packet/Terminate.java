package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;
//	
//	Terminate (F)
//	Byte1('X')
//	标识消息是一个终止消息。
//	
//	Int32(4)
//	以字节记的消息内容的长度，包括长度自身。

import io.mycat.backend.postgresql.utils.PIOUtils;

/***
 * 终止命令
 * 
 * @author Coollf
 *
 */
public class Terminate extends PostgreSQLPacket {

	private int length = 4;

	@Override
	public int getLength() {

		return length;
	}

	@Override
	public char getMarker() {
		return PacketMarker.F_Terminate.getValue();
	}

	public void write(ByteBuffer buffer) {
		PIOUtils.SendChar(getMarker(), buffer);
		PIOUtils.SendInteger4(getLength(), buffer);
	}

}
