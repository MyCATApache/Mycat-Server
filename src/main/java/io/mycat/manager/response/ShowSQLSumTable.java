package io.mycat.manager.response;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.statistic.stat.TableStat;
import io.mycat.statistic.stat.TableStatAnalyzer;
import io.mycat.util.LongUtil;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.List;

public class ShowSQLSumTable {
	
	private static DecimalFormat decimalFormat = new DecimalFormat("0.00");

    private static final int FIELD_COUNT = 8;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("TABLE", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("R", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("W", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("R%", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("RELATABLE", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("RELACOUNT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("LAST_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c, boolean isClear) {
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
        
        /*
        int i=0;
        Map<String, TableStat> statMap = TableStatAnalyzer.getInstance().getTableStatMap();
        for (TableStat tableStat : statMap.values()) {
        	i++;
           RowDataPacket row = getRow(tableStat,i, c.getCharset());//getRow(sqlStat,sql, c.getCharset());
           row.packetId = ++packetId;
           buffer = row.write(buffer, c,true);
        }
        */
        List<TableStat> list = TableStatAnalyzer.getInstance().getTableStats(isClear);
        if ( list != null ) {
            int i = 1;
	        for (TableStat tableStat : list) {
                if(tableStat!=null){
	                RowDataPacket row = getRow(tableStat,i, c.getCharset());
                    i++;
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

    private static RowDataPacket getRow(TableStat tableStat, long idx, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        if (tableStat == null){
        	row.add(StringUtil.encode(("not fond"), charset));
        	return row;
        }
        
        String table = tableStat.getTable();
        long R = tableStat.getRCount();
        long W = tableStat.getWCount();
        String __R = decimalFormat.format( 1.0D * R / (R + W) );
        
        
        StringBuffer relaTableNameBuffer = new StringBuffer();
        StringBuffer relaTableCountBuffer = new StringBuffer();
        List<TableStat.RelaTable> relaTables = tableStat.getRelaTables();
        if ( !relaTables.isEmpty() ) { 
        	
	        for(TableStat.RelaTable relaTable: relaTables) {
	        	relaTableNameBuffer.append( relaTable.getTableName() ).append(", ");
	        	relaTableCountBuffer.append( relaTable.getCount() ).append(", ");
	        }
	        
        } else {
        	relaTableNameBuffer.append("NULL");
        	relaTableCountBuffer.append("NULL");
        }
        
        row.add( StringUtil.encode( table, charset) );
        row.add( LongUtil.toBytes( R ) );
        row.add( LongUtil.toBytes( W ) );
        row.add( StringUtil.encode( String.valueOf( __R ), charset) );
        row.add( StringUtil.encode( relaTableNameBuffer.toString(), charset) );
        row.add( StringUtil.encode( relaTableCountBuffer.toString(), charset) );
        row.add( LongUtil.toBytes( tableStat.getLastExecuteTime() ) );
        
        return row;
    }

}
