package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

/**
 * 后端数据包信息
 * 
 * @author Coollf
 *
 */

// BackendKeyData (B)
// Byte1('K')
// 标识该消息是一个取消键字数据。 如果前端希望能够在稍后发出 CancelRequest 消息， 那么它必须保存这个值。
//
// Int32(12)
// 以字节记的消息内容的长度，包括长度本身。
//
// Int32
// 后端的进程号（PID）。
//
// Int32
// 此后端的密钥（secret key ）。
public class BackendKeyData extends PostgreSQLPacket {
	/**
	 * 长度
	 */
	private int length;

	/***
	 * 进程ID
	 */
	private int pid;

	/***
	 * 此后端的密钥（secret key ）
	 */
	private int secretKey;

	public int getPid() {
		return pid;
	}

	public int getSecretKey() {
		return secretKey;
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return PacketMarker.B_BackendKey.getValue();
	}

	/***
	 * 解析数据包
	 * 
	 * @param buffer
	 * @param offset
	 * @return
	 * @throws IllegalArgumentException
	 */
	public static BackendKeyData parse(ByteBuffer buffer, int offset) {
		if (buffer.get(offset) != PacketMarker.B_BackendKey.getValue()) {
			throw new IllegalArgumentException("this packet not is BackendKeyData");
		}
		BackendKeyData pac = new BackendKeyData();
		pac.length = buffer.getInt(offset + 1);
		pac.pid = buffer.getInt(offset + 1 + 4);
		pac.secretKey = buffer.getInt(offset + 1 + 4 + 4);
		return pac;
	}

}
