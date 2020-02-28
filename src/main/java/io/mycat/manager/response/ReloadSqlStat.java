package io.mycat.manager.response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.SystemConfig;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.OkPacket;

public class ReloadSqlStat {

    private static final Logger logger = LoggerFactory.getLogger(ReloadSqlStat.class);

    public static void execute(ManagerConnection c, String openCloseFlag) {
        SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
        int oldStat = system.getUseSqlStat();
        int newStat = oldStat;
        if ("open".equalsIgnoreCase(openCloseFlag)) {
            newStat = 1;
        } else if ("close".equalsIgnoreCase(openCloseFlag)) {
            newStat = 0;
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "reload @@sqlstat=open|close");
            return;
        }

        system.setUseSqlStat(newStat);
        MycatServer.getInstance().ensureSqlstatRecycleFuture();

        StringBuilder s = new StringBuilder();
        s.append(c).append("Reset  @@sqlstat=" + openCloseFlag + " success by manager");

        logger.warn(s.toString());

        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = oldStat != newStat ? 1 : 0;
        ok.serverStatus = 2;
        ok.message = "Reset  @@sqlstat success".getBytes();
        ok.write(c);
        System.out.println(s.toString());
    }

}