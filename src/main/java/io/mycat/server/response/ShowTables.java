package io.mycat.server.response;

import io.mycat.MycatConfig;
import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.mysql.PacketUtil;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * show tables impl
 * @author yanglixue
 *
 */
public class ShowTables { 

    private static final int FIELD_COUNT = 1;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    private static final String SCHEMA_KEY = "schemaName";
    private static final String LIKE_KEY = "like";
    private static final   Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s+(TABLES)(\\s+(FROM)\\s+([a-zA-Z_0-9]+))?(\\s+(LIKE\\s+'(.*)'))?\\s*",Pattern.CASE_INSENSITIVE);
	
	/**
	 * response method.
	 * @param c
	 */
	public static void response(ServerConnection c,String stmt,int type) {
		SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(c.getSchema());
        if(schema != null) {
        	//不分库的schema，show tables从后端 mysql中查
            if(schema.isNoSharding()) {
            	c.execute(stmt, ServerParse.SHOW);
                return;
            }
        } else {
             c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,"No database selected");
        }

        //分库的schema，直接从SchemaConfig中获取所有表名
        Map<String,String> parm = buildFields(c,stmt);
	  
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
        MycatConfig conf = MycatServer.getInstance().getConfig(); 
        
        Map<String, UserConfig> users = conf.getUsers();
        UserConfig user = users == null ? null : users.get(c.getUser());
        if (user != null) {
	        TreeSet<String> tableSet = new TreeSet<String>();
	        
	        Map<String, SchemaConfig> schemas = conf.getSchemas();
	        for (String name:schemas.keySet()){
	        	if (null !=parm.get(SCHEMA_KEY) && parm.get(SCHEMA_KEY).toUpperCase().equals(name.toUpperCase())  ){

	        		if(null==parm.get("LIKE_KEY")){
	        			tableSet.addAll(schemas.get(name).getTables().keySet());
	        		}else{
	        			String p = "^" + parm.get("LIKE_KEY").replaceAll("%", ".*");
	        			Pattern pattern = Pattern.compile(p,Pattern.CASE_INSENSITIVE);
	        			Matcher ma ;
	        			
	        			for (String tname : schemas.get(name).getTables().keySet()){
	        				ma=pattern.matcher(tname);
	        				if(ma.matches()){
	        					tableSet.add(tname);
	        				}
	        			}
	        			
	        		}
	        		
	        	}
	        };
	        
	        
	        for (String name : tableSet) {
		        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		        row.add(StringUtil.encode(name.toLowerCase(), c.getCharset()));
		        row.packetId = ++packetId;
		        buffer = row.write(buffer, c,true);
	        } 
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // post write
        c.write(buffer);
		
		
    }
	
	/**
	 * build fields
	 * @param c
	 * @param stmt
	 */
	private static Map<String,String> buildFields(ServerConnection c,String stmt) {
	 
		Map<String,String> map = new HashMap<String, String>();

		Matcher ma = pattern.matcher(stmt);

		if(ma.find()){
			  String schemaName=ma.group(5);
			  if (null !=schemaName && (!"".equals(schemaName)) && (!"null".equals(schemaName))){
				  map.put(SCHEMA_KEY, schemaName);
			  }
			  
			 String like = ma.group(8);
			 if (null !=like && (!"".equals(like)) && (!"null".equals(like))){
				  map.put("LIKE_KEY", like);
			  }
			}


		if(null==map.get(SCHEMA_KEY)){
			map.put(SCHEMA_KEY, c.getSchema());
		}
		 
		
		int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("Tables in " + map.get(SCHEMA_KEY), Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        eof.packetId = ++packetId;
         
        return  map;
        
	}

	
}
