package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.utils.PIOUtils;

public class ParseComplete extends PostgreSQLPacket {
	private char marker = PacketMarker.B_ParseComplete.getValue();
	private int length ;
	
	
	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	public static ParseComplete parse(ByteBuffer buffer, int offset) {
		if ((char) buffer.get(offset) != PacketMarker.B_ParseComplete.getValue()) {
			throw new IllegalArgumentException("this packet not is ParseComplete");
		}
		ParseComplete parse = new ParseComplete();
		parse.length = PIOUtils.redInteger4(buffer, offset+1);
		return parse;
	}

}
