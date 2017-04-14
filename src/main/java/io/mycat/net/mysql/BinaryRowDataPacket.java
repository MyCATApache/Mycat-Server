package io.mycat.net.mysql;


import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import io.mycat.backend.mysql.BufferUtil;
import io.mycat.config.Fields;
import io.mycat.memory.unsafe.row.UnsafeRow;
import io.mycat.net.FrontendConnection;
import io.mycat.util.ByteUtil;
import io.mycat.util.DateUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @see @http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html#packet-ProtocolBinary::ResultsetRow
 * @see @http://dev.mysql.com/doc/internals/en/binary-protocol-value.html
 * @author CrazyPig
 * 
 */
public class BinaryRowDataPacket extends MySQLPacket {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinaryRowDataPacket.class);
	public int fieldCount;
	public List<byte[]> fieldValues;
	public byte packetHeader = (byte) 0;
	public byte[] nullBitMap;
	
	public List<FieldPacket> fieldPackets;
	
	public BinaryRowDataPacket() {}
	
	/**
	 * 从UnsafeRow转换成BinaryRowDataPacket
	 * 
	 * 说明: 当开启<b>isOffHeapuseOffHeapForMerge</b>参数时,会使用UnsafeRow封装数据,
	 * 因此需要从这个对象里面将数据封装成BinaryRowDataPacket
	 * 
	 * @param fieldPackets
	 * @param unsafeRow
	 */
	public void read(List<FieldPacket> fieldPackets, UnsafeRow unsafeRow) {
		this.fieldPackets = fieldPackets;
		this.fieldCount = unsafeRow.numFields();
		this.fieldValues = new ArrayList<byte[]>(fieldCount);
		this.packetId = unsafeRow.packetId;
		this.nullBitMap = new byte[(fieldCount + 7 + 2) / 8];
		
		for(int i = 0; i < this.fieldCount; i++) {
			byte[] fv = unsafeRow.getBinary(i);
			FieldPacket fieldPk = fieldPackets.get(i);
			if(fv == null) {
				storeNullBitMap(i);
				this.fieldValues.add(fv);
			} else {
				convert(fv, fieldPk);
			}
		}
	}
	
	/**
	 * 从RowDataPacket转换成BinaryRowDataPacket
	 * @param fieldPackets 字段包集合
	 * @param rowDataPk 文本协议行数据包
	 */
	public void read(List<FieldPacket> fieldPackets, RowDataPacket rowDataPk) {
		this.fieldPackets = fieldPackets;
		this.fieldCount = rowDataPk.fieldCount;
		this.fieldValues = new ArrayList<byte[]>(fieldCount);
		this.packetId = rowDataPk.packetId;
		this.nullBitMap = new byte[(fieldCount + 7 + 2) / 8];
		
		List<byte[]> _fieldValues = rowDataPk.fieldValues;
		for (int i = 0; i < fieldCount; i++) {
			byte[] fv = _fieldValues.get(i);
			FieldPacket fieldPk = fieldPackets.get(i);
			if (fv == null) { // 字段值为null,根据协议规定存储nullBitMap
				storeNullBitMap(i);
				this.fieldValues.add(fv);
			} else {
				convert(fv, fieldPk);
			}
		}
	}
	
	private void storeNullBitMap(int i) {
		int bitMapPos = (i + 2) / 8;
		int bitPos = (i + 2) % 8;
		this.nullBitMap[bitMapPos] |= (byte) (1 << bitPos);
	}
	
	/**
	 * 从RowDataPacket的fieldValue的数据转化成BinaryRowDataPacket的fieldValue数据
	 * @param fv
	 * @param fieldPk
	 */
	private void convert(byte[] fv, FieldPacket fieldPk) {
		
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
			this.fieldValues.add(fv);
			break;
		case Fields.FIELD_TYPE_LONGLONG:
			// Fields
			// value (8) -- integer

			// Example
			// 01 00 00 00 00 00 00 00 -- int64 = 1
			long longVar = ByteUtil.getLong(fv);
			this.fieldValues.add(ByteUtil.getBytes(longVar));
			break;
		case Fields.FIELD_TYPE_LONG:
		case Fields.FIELD_TYPE_INT24:
			// Fields
			// value (4) -- integer

			// Example
			// 01 00 00 00 -- int32 = 1
			int intVar = ByteUtil.getInt(fv);
			this.fieldValues.add(ByteUtil.getBytes(intVar));
			break;
		case Fields.FIELD_TYPE_SHORT:
		case Fields.FIELD_TYPE_YEAR:
			// Fields
			// value (2) -- integer

			// Example
			// 01 00 -- int16 = 1
			short shortVar = ByteUtil.getShort(fv);
			this.fieldValues.add(ByteUtil.getBytes(shortVar));
			break;
		case Fields.FIELD_TYPE_TINY:
			// Fields
			// value (1) -- integer

			// Example
			// 01 -- int8 = 1
			int tinyVar = ByteUtil.getInt(fv);
			byte[] bytes = new byte[1];
			bytes[0] = (byte)tinyVar;
			this.fieldValues.add(bytes);
			break;
		case Fields.FIELD_TYPE_DOUBLE:
			// Fields
			// value (string.fix_len) -- (len=8) double

			// Example
			// 66 66 66 66 66 66 24 40 -- double = 10.2
			double doubleVar = ByteUtil.getDouble(fv);
			this.fieldValues.add(ByteUtil.getBytes(doubleVar));
			break;
		case Fields.FIELD_TYPE_FLOAT:
			// Fields
			// value (string.fix_len) -- (len=4) float

			// Example
			// 33 33 23 41 -- float = 10.2
			float floatVar = ByteUtil.getFloat(fv);
			this.fieldValues.add(ByteUtil.getBytes(floatVar));
			break;
		case Fields.FIELD_TYPE_DATE:
			try {
				Date dateVar = DateUtil.parseDate(ByteUtil.getDate(fv), DateUtil.DATE_PATTERN_ONLY_DATE);
				this.fieldValues.add(ByteUtil.getBytes(dateVar, false));				
			} catch(org.joda.time.IllegalFieldValueException e1) {
				// 当时间为 0000-00-00 00:00:00 的时候, 默认返回 1970-01-01 08:00:00.0
				this.fieldValues.add(ByteUtil.getBytes(new Date(0L), false));
			} catch (ParseException e) {
				LOGGER.error("error",e);
			}
			break;
		case Fields.FIELD_TYPE_DATETIME:
		case Fields.FIELD_TYPE_TIMESTAMP:
			String dateStr = ByteUtil.getDate(fv);
			Date dateTimeVar = null;
			try {
				if (dateStr.indexOf(".") > 0) {
					dateTimeVar = DateUtil.parseDate(dateStr, DateUtil.DATE_PATTERN_FULL);
					this.fieldValues.add(ByteUtil.getBytes(dateTimeVar, false));
				} else {
					dateTimeVar = DateUtil.parseDate(dateStr, DateUtil.DEFAULT_DATE_PATTERN);
					this.fieldValues.add(ByteUtil.getBytes(dateTimeVar, false));
				}
			} catch(org.joda.time.IllegalFieldValueException e1) {
				// 当时间为 0000-00-00 00:00:00 的时候, 默认返回 1970-01-01 08:00:00.0
				this.fieldValues.add(ByteUtil.getBytes(new Date(0L), false));
				
			} catch (ParseException e) {
				LOGGER.error("error",e);
			}
			break;
		case Fields.FIELD_TYPE_TIME:
			String timeStr = ByteUtil.getTime(fv);
			Date timeVar = null;
			try {
				if (timeStr.indexOf(".") > 0) {
					timeVar = DateUtil.parseDate(timeStr, DateUtil.TIME_PATTERN_FULL);
					this.fieldValues.add(ByteUtil.getBytes(timeVar, true));
				} else {
					timeVar = DateUtil.parseDate(timeStr, DateUtil.DEFAULT_TIME_PATTERN);
					this.fieldValues.add(ByteUtil.getBytes(timeVar, true));
				}
				
			} catch(org.joda.time.IllegalFieldValueException e1) {
				// 当时间为 0000-00-00 00:00:00 的时候, 默认返回 1970-01-01 08:00:00.0
				this.fieldValues.add(ByteUtil.getBytes(new Date(0L), true));
				
			} catch (ParseException e) {
				LOGGER.error("error",e);
			}
			break;
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
	public ByteBuffer write(ByteBuffer bb, FrontendConnection c,
			boolean writeSocketIfFull) {
		int size = calcPacketSize();
		int packetHeaderSize = c.getPacketHeaderSize();
		int totalSize = size + packetHeaderSize;
		bb = c.checkWriteBuffer(bb, totalSize, writeSocketIfFull);
		BufferUtil.writeUB3(bb, size);
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
		return bb;
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
					/*
					 * 长度编码的字符串需要计算存储长度, 根据mysql协议文档描述
					 * To convert a length-encoded integer into its numeric value, check the first byte:
					 * If it is < 0xfb, treat it as a 1-byte integer.
                     * If it is 0xfc, it is followed by a 2-byte integer.
                     * If it is 0xfd, it is followed by a 3-byte integer.
                     * If it is 0xfe, it is followed by a 8-byte integer.
					 * 
					 */
					if(value.length != 0) {
						/*
						 * 长度编码的字符串需要计算存储长度,不能简单默认只有1个字节是表示长度,当数据足够长,占用的就不止1个字节
						 */
//						size = size + 1 + value.length;
						size = size + BufferUtil.getLength(value);
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
