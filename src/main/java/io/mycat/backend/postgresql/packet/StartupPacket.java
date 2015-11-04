package io.mycat.backend.postgresql.packet;

import java.util.List;

public class StartupPacket extends PostgreSQLPacket{
	public int major; //协议版本
	
	public List<String[]> params; //协议参数

	@Override
	public int getLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public char getMarker() {
		// TODO Auto-generated method stub
		return 0;
	}
}
