package io.mycat.sqlengine.response;

import io.mycat.backend.BackendConnection;
import io.mycat.config.Fields;
import io.mycat.mysql.util.PacketUtil;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.FieldPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.net.buffer.BufferArray;
import io.mycat.net.nio.Connection;
import io.mycat.net.nio.NetSystem;
import io.mycat.mysql.MySQLFrontConnection;
import io.mycat.session.NonBlockingSession;
import io.mycat.util.StringUtil;

import java.util.Collection;

/**
 * show front session detail info
 * 
 * @author wuzhih
 * 
 */
public class ShowSession {
	private static final int FIELD_COUNT = 3;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("SESSION", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("DN_COUNT", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("DN_LIST", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void execute(MySQLFrontConnection c) {
		BufferArray bufferArray = NetSystem.getInstance().getBufferPool()
				.allocateArray();
		// write header
		header.write(bufferArray);

		// write fields
		for (FieldPacket field : fields) {
			field.write(bufferArray);
		}

		// write eof
		eof.write(bufferArray);

		// write rows
		byte packetId = eof.packetId;
		for (Connection con : NetSystem.getInstance().getAllConnectios()
				.values()) {
			if (con instanceof MySQLFrontConnection) {
				RowDataPacket row = getRow((MySQLFrontConnection) con, c.getCharset());
				if (row != null) {
					row.packetId = ++packetId;
					row.write(bufferArray);
				}
			}
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		lastEof.write(bufferArray);

		// write buffer
		c.write(bufferArray);
	}

	private static RowDataPacket getRow(MySQLFrontConnection sc, String charset) {
		StringBuilder sb = new StringBuilder();
		NonBlockingSession ssesion = sc.getSession2();
		Collection<BackendConnection> backConnections = ssesion.getTargetMap()
				.values();
		int cncount = backConnections.size();
		if (cncount == 0) {
			return null;
		}
		for (BackendConnection backCon : backConnections) {
			sb.append(backCon).append("\r\n");
		}
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(sc.getId() + "", charset));
		row.add(StringUtil.encode(cncount + "", charset));
		row.add(StringUtil.encode(sb.toString(), charset));
		return row;
	}
}
