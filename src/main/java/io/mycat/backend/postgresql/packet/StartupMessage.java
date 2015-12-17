package io.mycat.backend.postgresql.packet;

import java.util.List;

public class StartupMessage extends PostgreSQLPacket {
	private char marker = PacketMarker.F_StartupMessage.getValue(); //标准
	public int major; // 协议版本

	public List<String[]> params; // 协议参数

	@Override
	public int getLength() {
		return 0;
	}

	@Override
	@Deprecated
	public char getMarker() {
		return marker;
	}
}
