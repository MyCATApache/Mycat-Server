package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.util.Map;

import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.stat.HighFrequencySqlAnalyzer;
import org.opencloudb.stat.HighFrequencySqlAnalyzer.SqlFrequency;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.StringUtil;

/**
 * 查询高频 SQL
 * 
 * @author zhuam
 *
 */
public final class ShowSQLHigh {
	
	private static final int FIELD_COUNT = 4;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("FREQUENCY", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }

        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        byte packetId = eof.packetId;        
        Map<String, SqlFrequency> sqlMap = HighFrequencySqlAnalyzer.getInstance().getSqlFrequency();
        int i = 0;
        for (SqlFrequency sqlFrequency : sqlMap.values()) {
        	i++;
        	RowDataPacket row = getRow(i, sqlFrequency.getSql(), sqlFrequency.getCount(), sqlFrequency.getLastTime(), c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer, c,true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(int i, String sql, int count, long lastTime, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add( LongUtil.toBytes( i ) );
        row.add( StringUtil.encode(sql, charset) );
        row.add( LongUtil.toBytes( count ) );
        row.add( LongUtil.toBytes( lastTime ) );
        return row;
    }


}
