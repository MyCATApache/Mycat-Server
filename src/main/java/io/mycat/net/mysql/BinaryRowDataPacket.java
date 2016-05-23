package io.mycat.net.mysql;


import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import io.mycat.backend.mysql.BufferUtil;
import io.mycat.config.Fields;
import io.mycat.net.FrontendConnection;
import io.mycat.util.ByteUtil;
import io.mycat.util.DateUtil;

/**
 * ProtocolBinary::ResultsetRow:
 * row of a binary resultset (COM_STMT_EXECUTE)

 * Payload
 * 1              packet header [00]
 * string[$len]   NULL-bitmap, length: (column_count + 7 + 2) / 8
 * string[$len]   values
 * 
 * A Binary Protocol Resultset Row is made up of the NULL bitmap 
 * containing as many bits as we have columns in the resultset + 2 
 * and the values for columns that are not NULL in the Binary Protocol Value format.
 * 
 * @see http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html#packet-ProtocolBinary::ResultsetRow
 * @see http://dev.mysql.com/doc/internals/en/binary-protocol-value.html
 * @author CrazyPig
 * 
 */
public class BinaryRowDataPacket extends MySQLPacket {
	
	public int fieldCount;
	public List<byte[]> fieldValues;
	public byte packetHeader = (byte) 0;
	public byte[] nullBitMap;
	
	public List<FieldPacket> fieldPackets;
	
	public BinaryRowDataPacket() {}
	
	/**
	 * 从RowDataPacket转换成BinaryRowDataPacket
	 * @param fieldPackets 字段包集合
	 * @param rowDataPk 文本协议行数据包
	 */
	public void read(List<FieldPacket> fieldPackets, RowDataPacket rowDataPk) {
		this.fieldPackets = fieldPackets;
		this.fieldCount = rowDataPk.fieldCount;
		this.fieldValues = new ArrayList<byte[]>(fieldCount);
		this.nullBitMap = new byte[(fieldCount + 7 + 2) / 8];
		
		List<byte[]> _fieldValues = rowDataPk.fieldValues;
		for (int i = 0; i < fieldCount; i++) {
			byte[] fv = _fieldValues.get(i);
			FieldPacket fieldPk = fieldPackets.get(i);
			if (fv == null) { // 字段值为null,根据协议规定存储nullBitMap
				int bitMapPos = (i + 2) / 8;
				int bitPos = (i + 2) % 8;
				this.nullBitMap[bitMapPos] |= (byte) (1 << bitPos);
				this.fieldValues.add(fv);
			} else {
				// 从RowDataPacket的fieldValue的数据转化成BinaryRowDataPacket的fieldValue数据
				int fieldType = fieldPk.type;
				switch (fieldType) {
				case Fields.FIELD_TYPE_STRING:
				case Fields.FIELD_TYPE_VARCHAR:
				case Fields.FIELD_TYPE_VAR_STRING:
				case Fields.FIELD_TYPE_ENUM:
				case Fields.FIELD_TYPE_SET:
				case Fields.FIELD_TYPE_LONG_BLOB:
				case Fields.FIELD_TYPE_MEDIUM_BLOB:
				case Fields.FIELD_TYPE_BLOB:
				case Fields.FIELD_TYPE_TINY_BLOB:
				case Fields.FIELD_TYPE_GEOMETRY:
				case Fields.FIELD_TYPE_BIT:
				case Fields.FIELD_TYPE_DECIMAL:
				case Fields.FIELD_TYPE_NEW_DECIMAL:
					// Fields
					// value (lenenc_str) -- string
					
					// Example
					// 03 66 6f 6f -- string = "foo"
					this.fieldValues.add(_fieldValues.get(i));
					break;
				case Fields.FIELD_TYPE_LONGLONG:
					// Fields
					// value (8) -- integer

					// Example
					// 01 00 00 00 00 00 00 00 -- int64 = 1
					long longVar = ByteUtil.getLong(_fieldValues.get(i));
					this.fieldValues.add(ByteUtil.getBytes(longVar));
					break;
				case Fields.FIELD_TYPE_LONG:
				case Fields.FIELD_TYPE_INT24:
					// Fields
					// value (4) -- integer

					// Example
					// 01 00 00 00 -- int32 = 1
					int intVar = ByteUtil.getInt(_fieldValues.get(i));
					this.fieldValues.add(ByteUtil.getBytes(intVar));
					break;
				case Fields.FIELD_TYPE_SHORT:
				case Fields.FIELD_TYPE_YEAR:
					// Fields
					// value (2) -- integer

					// Example
					// 01 00 -- int16 = 1
					short shortVar = ByteUtil.getShort(_fieldValues.get(i));
					this.fieldValues.add(ByteUtil.getBytes(shortVar));
					break;
				case Fields.FIELD_TYPE_TINY:
					// Fields
					// value (1) -- integer

					// Example
					// 01 -- int8 = 1
					int tinyVar = ByteUtil.getInt(_fieldValues.get(i));
					byte[] bytes = new byte[1];
					bytes[0] = new Integer(tinyVar).byteValue();
					this.fieldValues.add(bytes);
					break;
				case Fields.FIELD_TYPE_DOUBLE:
					// Fields
					// value (string.fix_len) -- (len=8) double

					// Example
					// 66 66 66 66 66 66 24 40 -- double = 10.2
					double doubleVar = ByteUtil.getDouble(_fieldValues.get(i));
					this.fieldValues.add(ByteUtil.getBytes(doubleVar));
					break;
				case Fields.FIELD_TYPE_FLOAT:
					// Fields
					// value (string.fix_len) -- (len=4) float

					// Example
					// 33 33 23 41 -- float = 10.2
					float floatVar = ByteUtil.getFloat(_fieldValues.get(i));
					this.fieldValues.add(ByteUtil.getBytes(floatVar));
					break;
				case Fields.FIELD_TYPE_DATE:
					try {
						Date dateVar = DateUtil.parseDate(
								ByteUtil.getDate(_fieldValues.get(i)),
								DateUtil.DATE_PATTERN_ONLY_DATE);
						this.fieldValues.add(ByteUtil.getBytes(dateVar, false));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					break;
				case Fields.FIELD_TYPE_DATETIME:
				case Fields.FIELD_TYPE_TIMESTAMP:
					String dateStr = ByteUtil.getDate(_fieldValues.get(i));
					Date dateTimeVar = null;
					try {
						if (dateStr.indexOf(".") > 0) {
							dateTimeVar = DateUtil.parseDate(dateStr,
									DateUtil.DATE_PATTERN_FULL);
							this.fieldValues.add(ByteUtil.getBytes(dateTimeVar,
									false));
						} else {
							dateTimeVar = DateUtil.parseDate(dateStr,
									DateUtil.DEFAULT_DATE_PATTERN);
							this.fieldValues.add(ByteUtil.getBytes(dateTimeVar,
									false));
						}
					} catch (ParseException e) {
						e.printStackTrace();
					}
					break;
				case Fields.FIELD_TYPE_TIME:
					String timeStr = ByteUtil.getTime(_fieldValues.get(i));
					Date timeVar = null;
					try {
						if (timeStr.indexOf(".") > 0) {
							timeVar = DateUtil.parseDate(timeStr,
									DateUtil.TIME_PATTERN_FULL);
							this.fieldValues.add(ByteUtil.getBytes(timeVar,
									true));
						} else {
							timeVar = DateUtil.parseDate(timeStr,
									DateUtil.DEFAULT_TIME_PATTERN);
							this.fieldValues.add(ByteUtil.getBytes(timeVar,
									true));
						}
					} catch (ParseException e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
	}
	
	public void write(FrontendConnection conn) {
		
		int size = calcPacketSize();
		int packetHeaderSize = conn.getPacketHeaderSize();
		int totalSize = size + packetHeaderSize;
		ByteBuffer bb = null;
		
		bb = conn.getProcessor().getBufferPool().allocate(totalSize);

		BufferUtil.writeUB3(bb, calcPacketSize());
		bb.put(packetId);
		bb.put(packetHeader); // packet header [00]
		bb.put(nullBitMap); // NULL-Bitmap
		for(int i = 0; i < fieldCount; i++) { // values
			byte[] fv = fieldValues.get(i);
			if(fv != null) {
				FieldPacket fieldPk = this.fieldPackets.get(i);
				int fieldType = fieldPk.type;
				switch(fieldType) {
				case Fields.FIELD_TYPE_STRING:
				case Fields.FIELD_TYPE_VARCHAR:
				case Fields.FIELD_TYPE_VAR_STRING:
				case Fields.FIELD_TYPE_ENUM:
				case Fields.FIELD_TYPE_SET:
				case Fields.FIELD_TYPE_LONG_BLOB:
				case Fields.FIELD_TYPE_MEDIUM_BLOB:
				case Fields.FIELD_TYPE_BLOB:
				case Fields.FIELD_TYPE_TINY_BLOB:
				case Fields.FIELD_TYPE_GEOMETRY:
				case Fields.FIELD_TYPE_BIT:
				case Fields.FIELD_TYPE_DECIMAL:
				case Fields.FIELD_TYPE_NEW_DECIMAL:
					// 长度编码的字符串需要一个字节来存储长度(0表示空字符串)
					BufferUtil.writeLength(bb, fv.length);
					break;
					default:
						break;
				}
				if(fv.length > 0) {
					bb.put(fv);
				} 
			}
		}
		conn.write(bb);
		
	}

	@Override
	public int calcPacketSize() {
		int size = 0;
		size = size + 1 + nullBitMap.length;
		for(int i = 0, n = fieldValues.size(); i < n; i++) {
			byte[] value = fieldValues.get(i);
			if(value != null) {
				FieldPacket fieldPk = this.fieldPackets.get(i);
				int fieldType = fieldPk.type;
				switch(fieldType) {
				case Fields.FIELD_TYPE_STRING:
				case Fields.FIELD_TYPE_VARCHAR:
				case Fields.FIELD_TYPE_VAR_STRING:
				case Fields.FIELD_TYPE_ENUM:
				case Fields.FIELD_TYPE_SET:
				case Fields.FIELD_TYPE_LONG_BLOB:
				case Fields.FIELD_TYPE_MEDIUM_BLOB:
				case Fields.FIELD_TYPE_BLOB:
				case Fields.FIELD_TYPE_TINY_BLOB:
				case Fields.FIELD_TYPE_GEOMETRY:
				case Fields.FIELD_TYPE_BIT:
				case Fields.FIELD_TYPE_DECIMAL:
				case Fields.FIELD_TYPE_NEW_DECIMAL:
					// 长度编码的字符串需要一个字节来存储长度
					if(value.length != 0) {
						size = size + 1 + value.length;
					} else {
						size = size + 1; // 处理空字符串,只计算长度1个字节
					}
					break;
					default:
						size = size + value.length;
						break;
				}
			}
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Binary RowData Packet";
	}
}
