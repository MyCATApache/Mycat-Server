package io.mycat.manager.response;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.statistic.stat.SqlResultSet;
import io.mycat.statistic.stat.UserStat;
import io.mycat.statistic.stat.UserStatAnalyzer;
import io.mycat.util.IntegerUtil;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * show 大结果集 SQL
 * 
 * @author songgw
 *
 */
public final class ShowSqlResultSet {
	
	private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("FREQUENCY", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("RESULTSET_SIZE", Fields.FIELD_TYPE_INT24);
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
        int i=0;
        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
    	for (UserStat userStat : statMap.values()) {
        	String user = userStat.getUser();        
        	ConcurrentHashMap<String, SqlResultSet> map=userStat.getSqlResultSizeRecorder().getSqlResultSet();
             if ( map != null ) { 
     	        for (SqlResultSet sqlResultSet:map.values()) {
     	        	RowDataPacket row = getRow(++i, user,sqlResultSet.getSql(), sqlResultSet.getCount(), sqlResultSet.getResultSetSize(),c.getCharset());
     	            row.packetId = ++packetId;
     	            buffer = row.write(buffer, c,true);
     	        }
             }
    	}    
        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(int i, String user,String sql, int count, int resultSetSize,String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add( LongUtil.toBytes( i ) );
        row.add( StringUtil.encode( user, charset) );
        row.add( LongUtil.toBytes( count ) );
        row.add( StringUtil.encode(sql, charset) );
        row.add( IntegerUtil.toBytes(resultSetSize) );
        return row;
    }


}
