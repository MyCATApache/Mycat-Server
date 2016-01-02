package org.opencloudb.response;


import org.apache.log4j.Logger;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.stat.QueryConditionAnalyzer;

public class ReloadQueryCf {
	
	private static final Logger logger = Logger.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c, String cf) {
    	
    	if ( cf == null ) 
    		cf = "NULL";
    	
    	QueryConditionAnalyzer.getInstance().setCf(cf);
    	
        StringBuilder s = new StringBuilder();
        s.append(c).append("Reset show  @@sql.condition="+ cf +" success by manager");
        
        logger.warn(s.toString());
        
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Reset show  @@sql.condition success".getBytes();
        ok.write(c);
        
        System.out.println(s.toString());
    }

}
