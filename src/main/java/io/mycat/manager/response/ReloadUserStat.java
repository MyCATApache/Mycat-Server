package io.mycat.manager.response;

import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.statistic.stat.UserStat;
import io.mycat.statistic.stat.UserStatAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class ReloadUserStat {
	
	private static final Logger logger = LoggerFactory.getLogger(ReloadUserStat.class);

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
