package org.opencloudb.response;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opencloudb.MycatServer;
import org.opencloudb.config.ErrorCode;
import org.opencloudb.config.Fields;
import org.opencloudb.config.model.QuarantineConfig;
import org.opencloudb.config.model.UserConfig;
import org.opencloudb.config.util.ConfigException;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.mysql.PacketUtil;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.FieldPacket;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.net.mysql.ResultSetHeaderPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ShowWhiteHost {
	private static final Logger LOGGER = LoggerFactory.getLogger(ShowWhiteHost.class);

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("IP", Fields.FIELD_TYPE_VARCHAR);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
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
        
		Map<String, List<UserConfig>> map=MycatServer.getInstance().getConfig().getQuarantine().getWhitehost();
		for (String key : map.keySet()) {  
			List<UserConfig> userConfigs=map.get(key);
			String users="";
			 for (int i = 0; i < userConfigs.size(); i++) {
				 if(i>0) users+=","+userConfigs.get(i).getName();
				 else
					 users+=userConfigs.get(i).getName(); 
			 }
            RowDataPacket row = getRow(key, users, c.getCharset());
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
    private static RowDataPacket getRow(String ip, String user, String charset) {        
    	RowDataPacket row = new RowDataPacket(FIELD_COUNT);         
        row.add( StringUtil.encode( ip, charset) );
        row.add( StringUtil.encode( user, charset) );
        return row;
    }
    public static String parseString(String stmt) {
   	 int offset = stmt.indexOf(',');
        if (offset != -1 && stmt.length() > ++offset) {
            String txt = stmt.substring(offset).trim();
            return txt;
        }
        return null;
   }    
	public static synchronized void setHost(ManagerConnection c,String ips) {
        OkPacket ok = new OkPacket();		
		String []users = ips.split(",");		
        if (users.length<2){
          c.writeErrMessage(ErrorCode.ER_YES, "white host info error.");
          return;
        }        
        String host="";
        List<UserConfig> userConfigs = new ArrayList<UserConfig>();
        int i=0;
        for(String user : users){
          if (i==0){
        	  host=user;
        	  i++;
          }
          else {
        	i++;  
        	UserConfig uc = MycatServer.getInstance().getConfig().getUsers().get(user);
            if (null == uc) {
            	c.writeErrMessage(ErrorCode.ER_YES, "user doesn't exist in host.");
                return; 
            }
            if (uc.getSchemas() == null || uc.getSchemas().size() == 0) {
            	c.writeErrMessage(ErrorCode.ER_YES, "host contains one root privileges user.");
                return;                 
            }
            userConfigs.add(uc);
          }   
        }  
       if (MycatServer.getInstance().getConfig().getQuarantine().addWhitehost(host, userConfigs)) {
    	   try{
               QuarantineConfig.updateToFile(host, userConfigs);
           }catch(Exception e){
        	   LOGGER.warn("set while host error : " + e.getMessage());
        	   c.writeErrMessage(ErrorCode.ER_YES, "white host set success ,but write to file failed :" + e.getMessage());
           }
    	   
           ok.packetId = 1;
           ok.affectedRows = 1;
           ok.serverStatus = 2;        
    	   ok.message = "white host set to succeed.".getBytes();	
           ok.write(c);  	 
           
       }
       else {
           c.writeErrMessage(ErrorCode.ER_YES, "host duplicated.");
       }
	}	
	
	
	
}
