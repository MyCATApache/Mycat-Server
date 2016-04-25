package org.opencloudb.response;

import java.util.Map;

import org.apache.log4j.Logger;
import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.stat.UserStat;
import org.opencloudb.stat.UserStatAnalyzer;

public class ReloadSqlSlowTime {
	private static final Logger logger = Logger.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c,long time) {
    	
    	//Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
    	UserStatAnalyzer.getInstance().setSlowTime(time);
       // for (UserStat userStat : statMap.values()) {
       // 	userStat.setSlowTime(time);
       // }
    	
        StringBuilder s = new StringBuilder();
        s.append(c).append("Reset show  @@sql.slow="+time+" time success by manager");
        
        logger.warn(s.toString());
        
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Reset show  @@sql.slow time success".getBytes();
        ok.write(c);
        System.out.println(s.toString());
    }

}