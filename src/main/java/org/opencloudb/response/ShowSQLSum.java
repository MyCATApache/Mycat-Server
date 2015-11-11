package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Map;

import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.stat.RWStat;
import org.opencloudb.stat.UserStat;
import org.opencloudb.stat.UserStatFilter;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.StringUtil;

/**
 * 查询用户的 SQL 执行情况
 * 
 * 1、用户 R/W 数 及读占比
 * 2、请求时间范围
 * 3、请求的耗时范围
 * 
 * @author zhuam
 */
public class ShowSQLSum {
	
	private static DecimalFormat decimalFormat = new DecimalFormat("0.00");

    private static final int FIELD_COUNT = 6;
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
        
        fields[i] = PacketUtil.getField("R/W, R%", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("22-06h, 06-13h, 13-18h, 18-22h", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;        
        
        fields[i] = PacketUtil.getField("<10ms, 10ms-200ms, 200ms-1s, >1s", Fields.FIELD_TYPE_VAR_STRING);
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
        int i=0;  
        
        Map<String, UserStat> statMap = UserStatFilter.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
        	i++;
           RowDataPacket row = getRow(userStat,i, c.getCharset());//getRow(sqlStat,sql, c.getCharset());
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

    private static RowDataPacket getRow(UserStat userStat, long idx, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        if (userStat == null){
        	row.add(StringUtil.encode(("not fond"), charset));
        	return row;
        }
        
        String user = userStat.getUser();
        RWStat rwStat = userStat.getRWStat();
        long RCOUNT = rwStat.getRCount();
        long WCOUNT = rwStat.getWCount();
        String RP = decimalFormat.format( 1.0D * RCOUNT / (RCOUNT + WCOUNT) );
        
        row.add( StringUtil.encode( user, charset) );
        row.add( StringUtil.encode( RCOUNT  + "/" + WCOUNT + ", " + RP, charset) );
        row.add( StringUtil.encode( rwStat.getExecuteHistogram().toString(), charset) );
        row.add( StringUtil.encode( rwStat.getTimeHistogram().toString(), charset) );
        row.add( LongUtil.toBytes( rwStat.getLastExecuteTime() ) );
        
        return row;
    }

}
