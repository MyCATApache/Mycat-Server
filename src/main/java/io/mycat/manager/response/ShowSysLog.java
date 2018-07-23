package io.mycat.manager.response;

import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.Fields;
import io.mycat.config.model.SystemConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.util.StringUtil;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Show @@SYSLOG LIMIT=50
 * 
 * @author zhuam
 * 
 */
public class ShowSysLog {
	
	private static final int FIELD_COUNT = 2;
	
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("DATE", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;
		
		fields[i] = PacketUtil.getField("LOG", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;
		
		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c, int numLines) {
		ByteBuffer buffer = c.allocate();

		// write header
		buffer = header.write(buffer, c, true);

		// write fields
		for (FieldPacket field : fields) {
			buffer = field.write(buffer, c, true);
		}

		// write eof
		buffer = eof.write(buffer, c, true);

        // write rows
        byte packetId = eof.packetId;
        
		String filename = SystemConfig.getHomePath()  + File.separator  + "logs" + File.separator + "mycat.log";
		
		String[] lines = getLinesByLogFile(filename, numLines);    
		
		boolean linesIsEmpty = true;
		for(int i = 0; i < lines.length ; i++){
	        String line = lines[i];
	        if ( line != null ) {	 	        	
	        	RowDataPacket row =  new RowDataPacket(FIELD_COUNT);
		        row.add(StringUtil.encode( line.substring(0,19), c.getCharset()));
		        row.add(StringUtil.encode( line.substring(19,line.length()), c.getCharset()));
		        row.packetId = ++packetId;
		        buffer = row.write(buffer, c,true);
		        
		        linesIsEmpty = false;
	        }
		}
		
		if ( linesIsEmpty ) {
			RowDataPacket row =  new RowDataPacket(FIELD_COUNT);
			row.add(StringUtil.encode( "NULL", c.getCharset()));
	        row.add(StringUtil.encode( "NULL", c.getCharset()));
	        row.packetId = ++packetId;
	        buffer = row.write(buffer, c,true);			
		}
		
		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}
	
	private static String[] getLinesByLogFile(String filename, int numLines) {
		

		String lines[] = new String[numLines];
		
		BufferedReader in = null;
	    try {
	    	//获取长度
	    	int start = 0;
	    	int totalNumLines = 0;
	    	 
	    	File logFile = new File(filename);  
		    in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));
		    
		    String line;
		    while ((line=in.readLine()) != null) {
		        totalNumLines++;
		    }
		    in.close();
		    
		    //
		    in = new BufferedReader(new InputStreamReader(new FileInputStream(logFile), "UTF-8"));
		   
		    // 跳过行
		    start = totalNumLines - numLines;
		    if (start < 0) { start = 0; }
		    for (int i=0; i<start; i++) {
		        in.readLine();
		    }
		    
		    // DESC		    
		    int i = 0;
		    int end = lines.length-1;
		    while ((line=in.readLine()) != null && i<numLines) {
		    	lines[end-i] = line;            
	        	i++;
	        }	        
		    numLines = start + i;
		    
	    } catch (FileNotFoundException ex) {
	    } catch (UnsupportedEncodingException e) {
		} catch (IOException e) {
		} finally {
			if ( in != null ) {
				try {
					in.close();
					in = null;
				} catch (IOException e) {
				}
			}
		}

		return lines;
	}	
	
}
