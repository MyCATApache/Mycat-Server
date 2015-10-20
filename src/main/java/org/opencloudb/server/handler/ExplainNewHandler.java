package org.opencloudb.server.handler;

import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.SchemaConfig;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.server.parser.ServerParse;
import org.opencloudb.util.StringUtil;

/**
 * 新的 Explain 实现
 * 
 * @author zhuam
 *
 */
public class ExplainNewHandler {

	private static final Logger logger = Logger.getLogger(ExplainHandler.class);
	
	private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[0];
	private static final int FIELD_COUNT = 12;
	
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	
	static {
		
		//主机信息
		fields[0] = PacketUtil.getField("data_node", Fields.FIELD_TYPE_VAR_STRING);
		fields[1] = PacketUtil.getField("sql", Fields.FIELD_TYPE_VAR_STRING);
		
		//实际DB执行信息
		fields[2] = PacketUtil.getField("id", Fields.FIELD_TYPE_LONG);
		fields[3] = PacketUtil.getField("select_type", Fields.FIELD_TYPE_VAR_STRING);
		fields[4] = PacketUtil.getField("table", Fields.FIELD_TYPE_VAR_STRING);
		fields[5] = PacketUtil.getField("type", Fields.FIELD_TYPE_VAR_STRING);
		fields[6] = PacketUtil.getField("possible_keys", Fields.FIELD_TYPE_VAR_STRING);
		fields[7] = PacketUtil.getField("key", Fields.FIELD_TYPE_VAR_STRING);
		fields[8] = PacketUtil.getField("key_len", Fields.FIELD_TYPE_LONG);
		fields[9] = PacketUtil.getField("ref", Fields.FIELD_TYPE_VAR_STRING);
		fields[10] = PacketUtil.getField("rows", Fields.FIELD_TYPE_LONG);
		fields[11] = PacketUtil.getField("Extra", Fields.FIELD_TYPE_VAR_STRING);
	}

	public static void handle(String stmt, ServerConnection c, int offset) {
		
		//String stmt1 = stmt.substring(offset);
		
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(c.getSchema());
		if(schema != null) {
        	//不分库的schema，从后端 mysql中查
            if(schema.isNoSharding()) {
            	c.execute(stmt, ServerParse.EXPLAIN);
                return;
            }
        } else {
             c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,"No database selected");
        }

		RouteResultset rrs = getRouteResultset(c, stmt);
		if (rrs == null)
			return;

		ByteBuffer buffer = c.allocate();

		// write header
		ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
		byte packetId = header.packetId;
		buffer = header.write(buffer, c,true);

		// write fields
		for (FieldPacket field : fields) {
			field.packetId = ++packetId;
			buffer = field.write(buffer, c,true);
		}

		// write eof
		EOFPacket eof = new EOFPacket();
		eof.packetId = ++packetId;
		buffer = eof.write(buffer, c,true);

		// write rows
		RouteResultsetNode[] rrsn = (rrs != null) ? rrs.getNodes()
				: EMPTY_ARRAY;
		for (RouteResultsetNode node : rrsn) {
			RowDataPacket row = getRow(node, c.getCharset());
			row.packetId = ++packetId;
			buffer = row.write(buffer, c,true);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c,true);

		// post write
		c.write(buffer);

	}

	private static RowDataPacket getRow(RouteResultsetNode node, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(node.getName(), charset));
		row.add(StringUtil.encode(node.getStatement(), charset));
		return row;
	}

	private static RouteResultset getRouteResultset(ServerConnection c,
			String stmt) {
		String db = c.getSchema();
		if (db == null) {
			c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
			return null;
		}
		SchemaConfig schema = MycatServer.getInstance().getConfig()
				.getSchemas().get(db);
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
					+ db + "'");
			return null;
		}
		try {
			int sqlType = ServerParse.parse(stmt) & 0xff;
			return MycatServer.getInstance().getRouterservice()
					.route(MycatServer.getInstance().getConfig().getSystem(),schema, sqlType, stmt, c.getCharset(), c);
		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			logger.warn(s.append(c).append(stmt).toString()+" error:"+ e);
			String msg = e.getMessage();
			c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e
					.getClass().getSimpleName() : msg);
			return null;
		}
	}

}