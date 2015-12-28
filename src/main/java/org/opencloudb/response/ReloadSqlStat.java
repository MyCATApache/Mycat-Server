package org.opencloudb.response;

import org.opencloudb.manager.ManagerConnection;
import org.opencloudb.net.mysql.OkPacket;
import org.opencloudb.stat.QueryResultDispatcher;

/**
 * 关闭/打开  统计模块
 * 
 * reload @@sqlstat=close;
 * reload @@sqlstat=open;
 * 
 * @author zhuam
 *
 */
public class ReloadSqlStat {
	
    public static void execute(ManagerConnection c, String stat) {
    	
    	boolean isOk = false;
    	
    	if ( stat != null ) { 
    	
    		if ( stat.equalsIgnoreCase("OPEN") ) {
    			isOk = QueryResultDispatcher.open();
    			
    		} else if ( stat.equalsIgnoreCase("CLOSE") ) {
    			isOk = QueryResultDispatcher.close();
    		}
	    	
    		StringBuffer sBuffer = new StringBuffer(35);
    		sBuffer.append( "Set sql stat module isclosed=").append( stat ).append(",");
    		sBuffer.append( (isOk == true ? " to succeed" : " to fail" ));
    		sBuffer.append( " by manager. ");
	        
	        OkPacket ok = new OkPacket();
	        ok.packetId = 1;
	        ok.affectedRows = 1;
	        ok.serverStatus = 2;
	        ok.message = sBuffer.toString().getBytes();
	        ok.write(c);
    	}
    }


}
