package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.utils.PIOUtils;

//		BindComplete (B)
//		Byte1('2')
//		标识消息为一个绑定结束标识符。
//		
//		Int32(4)
//		以字节记的消息长度，包括长度本身。


/***
 * 绑定预编译sql成功
 * @author Coollf
 *
 */
public class BindComplete extends PostgreSQLPacket {
	private char marker = PacketMarker.B_BindComplete.getValue();
	private int length;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	public static BindComplete parse(ByteBuffer buffer, int offset) {
		if ((char) buffer.get(offset) != PacketMarker.B_BindComplete.getValue()) {
			throw new IllegalArgumentException(
					"this packet not is BindComplete");
		}
		BindComplete parse = new BindComplete();
		parse.length = PIOUtils.redInteger4(buffer, offset + 1);
		return parse;
	}
}
