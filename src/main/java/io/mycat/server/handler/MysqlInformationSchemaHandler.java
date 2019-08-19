package io.mycat.server.handler;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.util.SchemaUtil;

import java.nio.ByteBuffer;


/**
 * 对 PhpAdmin's 控制台操作进行支持 
 * 
 * 如：SELECT * FROM information_schema.CHARACTER_SETS 等相关语句进行模拟返回
 * 
 * @author zhuam
 *
 */
public class MysqlInformationSchemaHandler {
	
	/**
	 * 写入数据包
	 * @param field_count
	 * @param fields
	 * @param c
	 */
	private static void doWrite(int field_count, FieldPacket[] fields, ServerConnection c) {
		
		ByteBuffer buffer = c.allocate();

		// write header
		ResultSetHeaderPacket header = PacketUtil.getHeader(field_count);
		byte packetId = header.packetId;
		buffer = header.write(buffer, c, true);

		// write fields
		for (FieldPacket field : fields) {
			field.packetId = ++packetId;
			buffer = field.write(buffer, c, true);
		}

		// write eof
		EOFPacket eof = new EOFPacket();
		eof.packetId = ++packetId;
		buffer = eof.write(buffer, c, true);

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// post write
		c.write(buffer);
		
	}
	
	public static void handle(String sql, ServerConnection c) {
		
		SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
		if ( schemaInfo != null ) {
			
			if ( schemaInfo.table.toUpperCase().equals("CHARACTER_SETS") ) {
				
				//模拟列头
				int field_count = 4;
			    FieldPacket[] fields = new FieldPacket[field_count];
			    fields[0] = PacketUtil.getField("CHARACTER_SET_NAME", Fields.FIELD_TYPE_VAR_STRING);
				fields[1] = PacketUtil.getField("DEFAULT_COLLATE_NAME", Fields.FIELD_TYPE_VAR_STRING);
			    fields[2] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
				fields[3] = PacketUtil.getField("MAXLEN", Fields.FIELD_TYPE_LONG);
				
				doWrite(field_count, fields, c);				
				
			} else {
				c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
			}			
			
		} else {
			c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
		}		
	}
}