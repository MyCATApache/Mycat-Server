package io.mycat.manager.response;

import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.statistic.stat.UserStat;
import io.mycat.statistic.stat.UserStatAnalyzer;

public class ReloadSqlSlowTime {
	private static final Logger logger = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c,long time) {
    	
    	Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
        	userStat.setSlowTime(time);
        }
    	
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