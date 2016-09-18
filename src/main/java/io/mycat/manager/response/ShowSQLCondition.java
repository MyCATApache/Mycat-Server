package io.mycat.manager.response;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.statistic.stat.QueryConditionAnalyzer;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

/**
 * SQL 查询条件 值统计
 * 
 * @author zhuam
 *
 */
public class ShowSQLCondition {
	
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
        
        fields[i] = PacketUtil.getField("KEY", Fields.FIELD_TYPE_VAR_STRING);        
        fields[i++].packetId = ++packetId;        
        
        fields[i] = PacketUtil.getField("VALUE", Fields.FIELD_TYPE_VAR_STRING);        
        fields[i++].packetId = ++packetId;  
        
        fields[i] = PacketUtil.getField("COUNT", Fields.FIELD_TYPE_LONGLONG);
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
        
        String key = QueryConditionAnalyzer.getInstance().getKey();
        List<Map.Entry<Object, AtomicLong>> list = QueryConditionAnalyzer.getInstance().getValues();
        if ( list != null  ) {       
        	
        	int size = list.size();
        	long total = 0L;
        	
	        for (int i = 0; i < size; i++) {
	        	Map.Entry<Object, AtomicLong> entry = list.get(i);
	        	Object value = entry.getKey();
	        	Long count = entry.getValue().get();
	        	total += count;
	        	
	        	RowDataPacket row = getRow(i, key, value.toString(), count, c.getCharset());
	            row.packetId = ++packetId;
	            buffer = row.write(buffer, c,true);
	        }
	        
        	RowDataPacket vk_row = getRow(size + 1, key + ".valuekey", "size", size, c.getCharset());
        	vk_row.packetId = ++packetId;
            buffer = vk_row.write(buffer, c,true);
            
        	RowDataPacket vc_row = getRow(size + 2, key + ".valuecount", "total", total, c.getCharset());
        	vc_row.packetId = ++packetId;
            buffer = vc_row.write(buffer, c,true);
	       
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(int i, String key, String value, long count, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add( LongUtil.toBytes( i ) );
        row.add( StringUtil.encode(key, charset) );
        row.add( StringUtil.encode(value, charset) );
        row.add( LongUtil.toBytes( count ) );
        return row;
    }


}
