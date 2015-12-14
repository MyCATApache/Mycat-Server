package org.opencloudb.response;

import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.stat.UserStat;
import org.opencloudb.stat.UserStatAnalyzer;

public final class ReloadUserStat {
	
	private static final Logger logger = Logger.getLogger(ReloadUserStat.class);

    public static void execute(ManagerConnection c) {
    	
    	Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
        	userStat.reset();
        }
    	
        StringBuilder s = new StringBuilder();
        s.append(c).append("Reset show @@sql  @@sql.sum  @@sql.slow success by manager");
        
        logger.warn(s.toString());
        
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Reset show @@sql  @@sql.sum @@sql.slow success".getBytes();
        ok.write(c);
    }

}
