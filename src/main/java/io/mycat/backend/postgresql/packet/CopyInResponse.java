package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;
//		CopyInResponse (B)
//		Byte1('G')
//		标识这条消息是一条 Start Copy In （开始拷贝进入）响应消息。 前端现在必须发送一条拷贝入数据。（如果还没准备好做这些事情， 那么发送一条 CopyFail 消息）。
//		
//		Int32
//		以字节记的消息内容的长度，包括长度本身。
//		
//		Int8
//		0 表示全部的 COPY 格式都是文本的（数据行由换行符分隔，字段由分隔字符分隔等等）。 1 表示全部 COPY 格式都是二进制的（类似 DataRow 格式）。 参阅 COPY	获取更多信息。
//		
//		Int16
//		数据中要拷贝的字段数（由下面的 N 解释）。
//		
//		Int16[N]
//		每个字段将要用的格式代码，目前每个都必须是零（文本）或者一（二进制）。 如果全部拷贝格式都是文本的，那么所有的都必须是零。

import io.mycat.backend.postgresql.utils.PIOUtils;

/***
 * 拷贝数据开始
 * 
 * @author Coollf
 *
 */
public class CopyInResponse extends PostgreSQLPacket {
	/**
	 * 标示
	 */
	private char marker = PacketMarker.B_CopyInResponse.getValue();

	/**
	 * 长度
	 */
	private int length;

	/**
	 * 拷贝协议, 0 文本, 1 二进制
	 */
	private DataProtocol protocol;

	/***
	 * 拷贝的数据字段数
	 */
	private short dataLength;

	/**
	 * @return the protocol
	 */
	public DataProtocol getProtocol() {
		return protocol;
	}

	/**
	 * @return the dataLength
	 */
	public short getDataLength() {
		return dataLength;
	}

	/**
	 * @return the columnType
	 */
	public DataProtocol[] getColumnType() {
		return columnType;
	}

	/**
	 * 要拷贝数据列的类型  Int16[N]
	 */
	private DataProtocol[] columnType;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	public static CopyInResponse parse(ByteBuffer buffer, int offset) {

		if (buffer.get(offset) != PacketMarker.B_CopyInResponse.getValue()) {
			throw new IllegalArgumentException(
					"this packetData not is CopyInResponse");
		}
		int _offset = offset + 1;
		CopyInResponse pack = new CopyInResponse();
		pack.length = PIOUtils.redInteger4(buffer, _offset);
		_offset += 4;
		pack.protocol = DataProtocol.valueOf(PIOUtils.redInteger1(buffer,
				_offset));
		_offset += 1;
		pack.dataLength = PIOUtils.redInteger2(buffer, _offset);
		_offset += 2;
		pack.columnType = new DataProtocol[pack.dataLength];
		for (int i = 0; i < pack.columnType.length; i++) {
			pack.columnType[i] = DataProtocol.valueOf(PIOUtils.redInteger2(
					buffer, _offset));
			_offset += 2;
		}
		return pack;
	}

}
