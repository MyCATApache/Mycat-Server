package io.mycat.backend.heartbeat.zkprocess;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.util.ZKUtils;

public class SwitchStatueToZK{
	public static final Logger LOGGER = LoggerFactory.getLogger(SwitchStatueToZK.class);

	private static  InterProcessMutex changingStatueLock;
	static {
		changingStatueLock =  new InterProcessMutex(ZKUtils.getConnection(),
				ZKUtils.getZKBasePath() +"heartbeat/changingStatueLock");
	}
	public static boolean startSwitch(String dataHost) {
		
		String path = ZKUtils.getZKBasePath() +"heartbeat/" + dataHost +"/";
		String changingResultPath = path + "changingStatue";
        Map<String, String> propertyMap = new HashMap<>();
        String myId = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);
        propertyMap.put(myId+"_changing_statue","switching now"); //状态
        propertyMap.put(myId + "_startTime",new Date().toString()); //切换开始时间
        propertyMap.put(myId + "_endTime", ""); //结束时间
        try{
        	try {
        		changingStatueLock.acquire(30000, TimeUnit.MILLISECONDS);
                ZKUtils.writeProperty(changingResultPath, propertyMap);
			} finally {
				changingStatueLock.release();
			}     
        	return true;
        }catch (Exception e) {
        	LOGGER.error(dataHost + " startSwitch err "   , e);
		}
        return false;
	}
	public static boolean endSwitch(String dataHost) {		
		String path = ZKUtils.getZKBasePath() +"heartbeat/" + dataHost +"/";
		String changingResultPath = path + "changingStatue";
        Map<String, String> propertyMap = new HashMap<>();
        String myId = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);
        propertyMap.put(myId+"_changing_statue","switching success"); //状态
        propertyMap.put(myId + "_endTime",new Date().toString()); //切换结束时间
        try{
        	try {
        		changingStatueLock.acquire(30000, TimeUnit.MILLISECONDS);
                ZKUtils.writeProperty(changingResultPath, propertyMap);
			} finally {
				changingStatueLock.release();
			}  
            return true;
        }catch (Exception e) {
        	LOGGER.error(dataHost + " endSwitch err "   , e);
            return false;
        }
        
	}

}
