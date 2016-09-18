package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.utils.PIOUtils;

/**********
 * 查询数据包
 * 
 * @author Coollf
 */
// Query (F)
// Byte1('Q')
// 标识消息是一个简单查询。
//
// Int32
// 以字节记的消息内容的长度，包括长度自身。
//
// String
// 查询字串自身。
public class Query extends PostgreSQLPacket {

	private String sql;

	@Override
	public int getLength() {
		return 4 + (sql == null ? 0 : (sql.getBytes(UTF8).length)); // length + string
															// length
	}

	@Override
	public char getMarker() {
		return PacketMarker.F_Query.getValue();
	}

	public Query(String sql) {
		this.sql = sql.trim() + "\0";
	}

	public void write(ByteBuffer buffer) {
		PIOUtils.SendChar(getMarker(), buffer);
		PIOUtils.SendInteger4(getLength(), buffer);
		PIOUtils.SendString(sql, buffer);
	}

}
