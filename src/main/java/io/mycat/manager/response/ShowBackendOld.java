package io.mycat.manager.response;

import java.nio.ByteBuffer;

import io.mycat.backend.BackendConnection;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.NIOProcessor;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;
import io.mycat.util.TimeUtil;

/**
 * 查询 reload @@config_all 后产生的后端连接（待回收）
 * 
 * @author zhuam
 */
public class ShowBackendOld {
	
	private static final int FIELD_COUNT = 10;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;
		fields[i] = PacketUtil.getField("id", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("mysqlId", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("host", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("port", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("l_port", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("net_in", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("net_out", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("life", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("lasttime", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;
		fields[i] = PacketUtil.getField("borrowed",Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c) {
		ByteBuffer buffer = c.allocate();
		buffer = header.write(buffer, c, true);
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}
		buffer = eof.write(buffer, c, true);
		byte packetId = eof.packetId;
		String charset = c.getCharset();
		
		for (BackendConnection bc : NIOProcessor.backends_old) {
			if ( bc != null) {
				RowDataPacket row = getRow(bc, charset);
				row.packetId = ++packetId;
				buffer = row.write(buffer, c, true);
			}
		}
		
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);
		c.write(buffer);
	}

	private static RowDataPacket getRow(BackendConnection c, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(LongUtil.toBytes(c.getId()));
		long threadId = 0;
		if (c instanceof MySQLConnection) {
			threadId = ((MySQLConnection) c).getThreadId();
		}
		row.add(LongUtil.toBytes(threadId));
		row.add(StringUtil.encode(c.getHost(), charset));
		row.add(IntegerUtil.toBytes(c.getPort()));
		row.add(IntegerUtil.toBytes(c.getLocalPort()));
		row.add(LongUtil.toBytes(c.getNetInBytes()));
		row.add(LongUtil.toBytes(c.getNetOutBytes()));
		row.add(LongUtil.toBytes((TimeUtil.currentTimeMillis() - c.getStartupTime()) / 1000L));
		row.add(LongUtil.toBytes( c.getLastTime() ));
		boolean isBorrowed = c.isBorrowed();
		row.add(isBorrowed ? "true".getBytes() : "false".getBytes());	
		return row;
	}

}
