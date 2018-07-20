package io.mycat.backend.postgresql.packet;

import io.mycat.backend.postgresql.utils.PIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

//		RowDescription (B)
//		Byte1('T')
//		标识消息是一个行描述。
//		
//		Int32
//		以字节记的消息内容的长度，包括长度自身。
//		
//		Int16
//		声明在一个行里面的字段数目（可以为零）。
//		
//		然后对于每个字段，有下面的东西：
//		
//		String
//		字段名字。
//		
//		Int32
//		如果字段可以标识为一个特定表的字段，那么就是表的对象 ID； 否则就是零。
//		
//		Int16
//		
//			如果该字段可以标识为一个特定表的字段，那么就是该表字段的属性号；否则就是零。
//		
//		Int32
//		字段数据类型的对象 ID。
//		
//		Int16
//		数据类型尺寸（参阅pg_type.typlen）。 请注意负数表示变宽类型。
//		
//		Int32
//		类型修饰词(参阅pg_attribut.atttypmod)。 修饰词的含义是类型相关的。
//		
//		Int16
//		
//			用于该字段的格式码。目前会是零（文本）或者一（二进制）。 从语句变种 Describe 返回的 RowDescription 里，格式码还是未知的，因此总是零。
public class RowDescription extends PostgreSQLPacket {

	private int length;

	/**
	 * 列数
	 */
	private short columnNumber;

	/***
	 * 列信息
	 */
	private ColumnDescription[] columns;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return PacketMarker.B_RowDescription.getValue();
	}

	public static RowDescription parse(ByteBuffer buffer, int offset)
			throws  IOException {
		if (buffer.get(offset) != PacketMarker.B_RowDescription.getValue()) {
			throw new IllegalArgumentException(
					"this packetData not is RowDescription");
		}
		RowDescription pack = new RowDescription();
		pack.length = PIOUtils.redInteger4(buffer, offset + 1);
		pack.columnNumber = PIOUtils.redInteger2(buffer, offset + 1 + 4);

		pack.columns = new ColumnDescription[pack.columnNumber];
		int _offset = offset + 1 + 4 + 2;
		for (int i = 0; i < pack.columns.length; i++) {
			ColumnDescription col = new ColumnDescription();
			col.columnName = PIOUtils.redString(buffer, _offset, UTF8);
			_offset = _offset + col.columnName.getBytes(UTF8).length + 1;

			col.oid = PIOUtils.redInteger4(buffer, _offset);
			_offset += 4;
			col.coid = PIOUtils.redInteger2(buffer, _offset);
			_offset += 2;
			col.columnType = DateType.valueOf(PIOUtils.redInteger4(buffer,
					_offset));			
			_offset += 4;
			col.typlen = PIOUtils.redInteger2(buffer, _offset);
			_offset += 2;
			col.atttypmod = PIOUtils.redInteger4(buffer, _offset);
			_offset += 4;
			col.protocol = DataProtocol.valueOf(PIOUtils.redInteger2(buffer,
					_offset));
			_offset += 2;
			pack.columns[i] = col;
		}

		return pack;
	}

	/**
	 * @return the columns
	 */
	public ColumnDescription[] getColumns() {
		return columns;
	}

	/**
	 * @return the columnNumber
	 */
	public short getColumnNumber() {
		return columnNumber;
	}

	public static class ColumnDescription {
		/***
		 * 列名称
		 */
		private String columnName;

		/**
		 * 表的对象id
		 */
		private int oid;

		/***
		 * 那么就是该表字段的属性号
		 */
		private short coid;

		/***
		 * 字段类型
		 */
		private DateType columnType;

		/**
		 * 数据类型尺寸
		 */
		private short typlen;

		/**
		 * 数尺寸,负数标示宽度类型
		 */
		private int atttypmod;

		/***
		 * 数据协议 int16
		 */
		private DataProtocol protocol;

		/**
		 * @return the columnName
		 */
		public String getColumnName() {
			return columnName;
		}

		/**
		 * @param columnName
		 *            the columnName to set
		 */
		public void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		/**
		 * @return the oid
		 */
		public int getOid() {
			return oid;
		}

		/**
		 * @param oid
		 *            the oid to set
		 */
		public void setOid(int oid) {
			this.oid = oid;
		}

		/**
		 * @return the coid
		 */
		public short getCoid() {
			return coid;
		}

		/**
		 * @param coid
		 *            the coid to set
		 */
		public void setCoid(short coid) {
			this.coid = coid;
		}

		/**
		 * @return the columnType
		 */
		public DateType getColumnType() {
			return columnType;
		}

		/**
		 * @param columnType
		 *            the columnType to set
		 */
		public void setColumnType(DateType columnType) {
			this.columnType = columnType;
		}

		/**
		 * @return the typlen
		 */
		public short getTyplen() {
			return typlen;
		}

		/**
		 * @param typlen
		 *            the typlen to set
		 */
		public void setTyplen(short typlen) {
			this.typlen = typlen;
		}

		/**
		 * @return the atttypmod
		 */
		public int getAtttypmod() {
			return atttypmod;
		}

		/**
		 * @param atttypmod
		 *            the atttypmod to set
		 */
		public void setAtttypmod(int atttypmod) {
			this.atttypmod = atttypmod;
		}

		/**
		 * @return the protocol
		 */
		public DataProtocol getProtocol() {
			return protocol;
		}

		/**
		 * @param protocol
		 *            the protocol to set
		 */
		public void setProtocol(DataProtocol protocol) {
			this.protocol = protocol;
		}
	}
}
