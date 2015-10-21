package org.opencloudb.stat;

import org.opencloudb.MycatServer;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.statistic.SQLRecord;

public class SqlSlowUtil {
	private final static int    SQL_SLOW_TIME = 1000;
	
	public static void SqlExecuteTime(long startTime,RouteResultset rrs) {
	    long executeTime=System.currentTimeMillis()-startTime;
	   // System.out.println( "执行时间 cost:" + executeTime+ "ms");
	    if (executeTime>=SQL_SLOW_TIME){
	    	SQLRecord record=new SQLRecord();
	    	//record.host
	    	//record.schema
	    	//record.dataNodeIndex
	    	record.executeTime=executeTime;
	    	record.dataNode    =getDataNodes(rrs.getNodes());
	    	record.statement     =rrs.getStatement();
	    	record.startTime      =startTime;	    	
	    	MycatServer.getInstance().getSqlRecorder().add(record);		    
	    }
	}
	
	private static  String getDataNodes(RouteResultsetNode[] rrn){		
		String dataNode="";
		for (int i=0;i<rrn.length;i++){
			dataNode+=rrn[i].getName()+",";
		}
		return dataNode;
	}

}
