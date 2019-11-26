package io.mycat.server.response;

import com.google.common.base.Strings;

import io.mycat.MycatServer;
import io.mycat.backend.mysql.PacketUtil;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.UserConfig;
import io.mycat.net.mysql.EOFPacket;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.ResultSetHeaderPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.util.SchemaUtil;
import io.mycat.util.StringUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * show tables impl
 * @author yanglixue
 *
 */
public class ShowFullTables
{

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    private static final String SCHEMA_KEY = "schemaName";
    //private static final String LIKE_KEY = "like";
    //private static final   Pattern pattern = Pattern.compile("^\\s*(SHOW)\\s++(FULL)*\\s*(TABLES)(\\s+(FROM)\\s+([a-zA-Z_0-9]+))?(\\s+(LIKE\\s+'(.*)'))?\\s*",Pattern.CASE_INSENSITIVE);

	/**
	 * response method.
	 * @param c
	 */
	public static void response(ServerConnection c,String stmt,int type) {
       String showSchemal= SchemaUtil.parseShowTableSchema(stmt) ;
        String cSchema =showSchemal==null? c.getSchema():showSchemal;
        SchemaConfig schema = MycatServer.getInstance().getConfig().getSchemas().get(cSchema);
        if(schema != null) {
        	//不分库的schema，show tables从后端 mysql中查
            String node = schema.getDataNode();
            if(!Strings.isNullOrEmpty(node)) {
            	c.execute(stmt, ServerParse.SHOW);
                return;
            }
        } else {
             c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,"No database selected");
        }

        //分库的schema，直接从SchemaConfig中获取所有表名
        Map<String,String> parm = buildFields(c,stmt);
        Set<String> tableSet = getTableSet(c, parm);


        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        fields[i] = PacketUtil.getField("Tables in " + parm.get(SCHEMA_KEY), Fields.FIELD_TYPE_VAR_STRING);
        fields[i].packetId = ++packetId;
        fields[i+1] = PacketUtil.getField("Table_type  " , Fields.FIELD_TYPE_VAR_STRING);
        fields[i+1].packetId = ++packetId;
        eof.packetId = ++packetId;
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
         packetId = eof.packetId;

        for (String name : tableSet) {
            RowDataPacket row = new RowDataPacket(FIELD_COUNT);
            row.add(StringUtil.encode(name.toLowerCase(), c.getCharset()));
            row.add(StringUtil.encode("BASE TABLE", c.getCharset()));
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

    public static Set<String> getTableSet(ServerConnection c, String stmt)
    {
        Map<String,String> parm = buildFields(c,stmt);
       return getTableSet(c, parm);

    }


    private static Set<String> getTableSet(ServerConnection c, Map<String, String> parm)
    {
        TreeSet<String> tableSet = new TreeSet<String>();
        MycatConfig conf = MycatServer.getInstance().getConfig();

        Map<String, UserConfig> users = conf.getUsers();
        UserConfig user = users == null ? null : users.get(c.getUser());
        if (user != null) {


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



        }
        return tableSet;
    }

    /**
	 * build fields
	 * @param c
	 * @param stmt
	 */
	private static Map<String,String> buildFields(ServerConnection c,String stmt) {
		Map<String,String> map = new HashMap<String, String>();
        String fields [] =SchemaUtil.parseShowTable(stmt);
        if (null !=fields[3] && (!"".equals(fields[3])) && (!"null".equals(fields[3]))){
            map.put(SCHEMA_KEY, fields[3]);
        }
        if ((fields[5] ==null || !"table_type".equals(fields[5])) && null !=fields[8] && (!"".equals(fields[8])) && (!"null".equals(fields[8]))){
            map.put("LIKE_KEY", fields[8]);
        }
		if(null==map.get(SCHEMA_KEY)){
			map.put(SCHEMA_KEY, c.getSchema());
		}
        return  map;
	}
}
