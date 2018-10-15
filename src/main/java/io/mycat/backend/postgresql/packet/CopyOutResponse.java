package io.mycat.backend.postgresql.packet;

import io.mycat.backend.postgresql.utils.PIOUtils;

import java.nio.ByteBuffer;

//		CopyOutResponse (B)
//		Byte1('H')
//		标识这条消息是一条 Start Copy Out （开始拷贝进出）响应消息。 这条消息后面将跟着一条拷贝出数据消息。
//		
//		Int32
//		以字节记的消息内容的长度，包括它自己。
//		
//		Int8
//		0 表示全部拷贝格式都是文本（数据行由换行符分隔， 字段由分隔字符分隔等等）。1 表示所有拷贝格式都是二进制的（类似于 DataRow 格式）。参阅 COPY 获取更多信息。
//		
//		Int16
//		要拷贝的数据的字段的数目（在下面的 N 说明）。
//		
//		Int16[N]
//		每个字段要试用的格式代码。目前每个都必须是零（文本）或者一（二进制）。 如果全部的拷贝格式都是文本，那么所有的都必须是零。
public class CopyOutResponse extends PostgreSQLPacket {
	/**
	 * 标示
	 */
	private char marker = PacketMarker.B_CopyOutResponse.getValue();

	/**
	 * 长度
	 */
	private int length;

	/**
	 * @return the marker
	 */
	public char getMarker() {
		return marker;
	}

	/**
	 * @return the length
	 */
	public int getLength() {
		return length;
	}

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
	 * 拷贝协议, 0 文本, 1 二进制
	 */
	private DataProtocol protocol;

	/***
	 * 拷贝的数据字段数
	 */
	private short dataLength;

	/**
	 * 要拷贝数据列的类型 Int16[N]
	 */
	private DataProtocol[] columnType;

	public static CopyOutResponse parse(ByteBuffer buffer, int offset) {

		if (buffer.get(offset) != PacketMarker.B_CopyOutResponse.getValue()) {
			throw new IllegalArgumentException(
					"this packetData not is CopyInResponse");
		}
		int _offset = offset + 1;
		CopyOutResponse pack = new CopyOutResponse();
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
