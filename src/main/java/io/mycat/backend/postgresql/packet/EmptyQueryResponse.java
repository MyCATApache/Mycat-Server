package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.utils.PIOUtils;


//		EmptyQueryResponse (B)
//		Byte1('I')
//		标识这条消息是对一个空查询字串的响应。 （这个消息替换了 CommandComplete。）
//		
//		Int32(4)
//		以字节记的消息内容长度，包括它自己。

/*******
 * 空查询响应
 * @author Coollf
 *
 */
public class EmptyQueryResponse extends PostgreSQLPacket {

	private char marker = PacketMarker.B_EmptyQueryResponse.getValue();
	private int length;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	public static EmptyQueryResponse parse(ByteBuffer buffer, int offset) {
		if (buffer.get(offset) != PacketMarker.B_EmptyQueryResponse.getValue()) {
			throw new IllegalArgumentException(
					"this packetData not is EmptyQueryResponse");
		}
		int _offset = offset + 1;
		EmptyQueryResponse pack = new EmptyQueryResponse();
		pack.length = PIOUtils.redInteger4(buffer, _offset);
		return pack;
	}

}
