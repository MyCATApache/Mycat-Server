package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

import org.opencloudb.config.Fields;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.stat.TableStat;
import org.opencloudb.stat.TableStatAnalyzer;
import org.opencloudb.util.LongUtil;
import org.opencloudb.util.StringUtil;

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
        List<Map.Entry<String, TableStat>> list = TableStatAnalyzer.getInstance().getTableStats(isClear);
        if ( list != null ) {        
	        for (int i = 0; i < list.size(); i++) {
	        	TableStat tableStat=list.get(i).getValue();
	            RowDataPacket row = getRow(tableStat,i, c.getCharset());
	            row.packetId = ++packetId;
	            buffer = row.write(buffer, c,true);	        	
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
