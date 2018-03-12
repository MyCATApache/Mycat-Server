package io.mycat.backend.postgresql.packet;

import java.nio.ByteBuffer;

import io.mycat.backend.postgresql.utils.PIOUtils;

//		DataRow (B)
//		Byte1('D')
//		标识这个消息是一个数据行。
//		
//		Int32
//		以字节记的消息内容的长度，包括长度自身。
//		
//		Int16
//		后面跟着的字段值的个数（可能是零）。
//		
//		然后，每个字段都会出现下面的数据域对：
//		
//		Int32
//		字段值的长度，以字节记（这个长度不包括它自己）。 可以为零。一个特殊的情况是，-1 表示一个 NULL 的字段值。 在 NULL 的情况下就没有跟着数据字段。
//		
//		Byten
//		一个字段的数值，以相关的格式代码表示的格式展现。 n 是上面的长度。
public class DataRow extends PostgreSQLPacket {
	public static class DataColumn {
		/**
		 * 字段值的长度，以字节记（这个长度不包括它自己）。 可以为零。一个特殊的情况是，-1 表示一个 NULL 的字段值。 在 NULL
		 * 的情况下就没有跟着数据字段。
		 */
		private int length;
		private byte[] data;

		private boolean isNull;

		/**
		 * @return the isNull
		 */
		public boolean isNull() {
			return isNull;
		}

		/**
		 * @return the length
		 */
		public int getLength() {
			return length;
		}

		/**
		 * @param length
		 *            the length to set
		 */
		public void setLength(int length) {
			this.length = length;
		}

		/**
		 * @return the data
		 */
		public byte[] getData() {
			return data;
		}

		/**
		 * @param data
		 *            the data to set
		 */
		public void setData(byte[] data) {
			this.data = data;
		}

	}

	/**
	 * 标准
	 */
	private char marker = PacketMarker.B_DataRow.getValue();

	/**
	 * 长度
	 */
	private int length;

	/**
	 * 列数
	 */
	private short columnNumber;

	/**
	 * @return the columnNumber
	 */
	public short getColumnNumber() {
		return columnNumber;
	}

	/**
	 * @return the columns
	 */
	public DataColumn[] getColumns() {
		return columns;
	}

	/**
	 * 数据列
	 */
	private DataColumn[] columns;

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public char getMarker() {
		return marker;
	}

	public static DataRow parse(ByteBuffer buffer, int offset) {

		if (buffer.get(offset) != PacketMarker.B_DataRow.getValue()) {
			throw new IllegalArgumentException("this packetData not is DataRow");
		}
		int _offset = offset + 1;
		DataRow pack = new DataRow();
		pack.length = PIOUtils.redInteger4(buffer, _offset);
		_offset += 4;
		pack.columnNumber = PIOUtils.redInteger2(buffer, _offset);
		_offset += 2;
		pack.columns = new DataColumn[pack.columnNumber];
		for (int i = 0; i < pack.columns.length; i++) {
			DataColumn col = new DataColumn();
			col.length = PIOUtils.redInteger4(buffer, _offset);
			_offset += 4;
			if (col.length == -1) {
				// 数据为空
				col.isNull = true;
			} else {
				col.data = PIOUtils.redByteArray(buffer, _offset, col.length);
				_offset += col.length;
			}
			pack.columns[i] = col;
		}
		return pack;
	}
}
