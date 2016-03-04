package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

public class ParameterStatus extends PostgreSQLPacket {
	/**
	 * 数据包长度
	 */
	private int length;

	private String key;

	private String value;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return PacketMarker.B_ParameterStatus.getValue();
	}

	public static ParameterStatus parse(ByteBuffer buffer, int offset) {
		if ((char) buffer.get(offset) != PacketMarker.B_ParameterStatus.getValue()) {
			throw new IllegalArgumentException("this packet not is ParameterStatus");
		}

		ParameterStatus ps = new ParameterStatus();
		ps.length = buffer.getInt(offset + 1);
		byte[] bs = new byte[ps.length - 4];
		for (int i = 0; i < bs.length; i++) {
			bs[i] = buffer.get(offset + 1 + 4 + i);
		}
		String _val = new String(bs, UTF8);
		String[] vs = _val.split(" ");
		ps.key = vs[0];
		ps.value = _val.substring(ps.key.length());
		return ps;
	}

	public String getKey() {
		return key;
	}

	public String getValue() {
		return value;
	}

}
