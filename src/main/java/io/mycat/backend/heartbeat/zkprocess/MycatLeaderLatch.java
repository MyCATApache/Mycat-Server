package io.mycat.backend.heartbeat.zkprocess;


/**
 * 重定义的leaderLatch 因为curator的选举存在着丢包的情况.
* 源文件名：MyLeaderLatch.java
* 文件版本：1.0.0
* 创建作者：zwy
* 创建日期：2018年5月20日
*/

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.backend.datasource.PhysicalDBPool;
import io.mycat.config.loader.zkprocess.comm.ZkConfig;
import io.mycat.config.loader.zkprocess.comm.ZkParamCfg;
import io.mycat.util.ZKUtils;
import io.netty.util.internal.ConcurrentSet;

public class MycatLeaderLatch {
	public static final Logger LOGGER = LoggerFactory.getLogger(MycatLeaderLatch.class);

//	Runnable isLeaderRunnable = null; //当选leader之后的回调方法
//	Runnable notLeaderRunnable = null; //失去leader之后的回调方法.
	private final String latchPath;
	volatile LeaderLatch latch;
	String myId;
	CuratorFramework client;
	int isLeaderCount = 0;
	int isSlaveCount = 0;
	volatile boolean isLeader;
	final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

	ConcurrentSet<ManageHeartBeatChange> manageHeartBeatChangeSet = new ConcurrentSet<>();
	public MycatLeaderLatch( String latchPath )  {		
		this.myId = ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID);
		this.latchPath = ZKUtils.getZKBasePath() + latchPath;
		this.client  = ZKUtils.getConnection();
		isLeader = false;
		//ZKUtils.createPath(this.latchPath, "");
		latch = new LeaderLatch(client, this.latchPath ,this.myId);	
		
		Map<String, PhysicalDBPool> dataSourceHosts = MycatServer.getInstance().getConfig().getDataHosts();
		try {
			for(String dataSource : dataSourceHosts.keySet()) {
				manageHeartBeatChangeSet.add(new ManageHeartBeatChange(this, dataSource));
			}
		} catch (Exception e) {
			LOGGER.warn("init ManageHeartBeatChange err:", e);
		}
		
	}
	//成为leader的回调方法
	private void isLeaderRunnable() {
		//
		LOGGER.debug(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID)  + " success to leader");
		for(ManageHeartBeatChange manageHeartBeatChange: manageHeartBeatChangeSet) {
			manageHeartBeatChange.leaderlisten();
		}
		
	}
	//不再是leader的回调方法
	private void notLeaderRunnable() {
		LOGGER.debug(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID)  +  " fail to leader, now is slave");
		for(ManageHeartBeatChange manageHeartBeatChange: manageHeartBeatChangeSet) {
			manageHeartBeatChange.stop();
		}
	}
	public void start() throws Exception {
		latch.start();
		
		service.scheduleAtFixedRate(checkIsLeader(), 0, 5, TimeUnit.SECONDS);
	}
	public void stop() throws IOException {
		latch.close();
		service.shutdown();
	}
 	private Runnable checkIsLeader() {
		return new Runnable() {			
			@Override
			public void run() {
				boolean isExist = false;
				try {
					Collection<Participant> participants = latch.getParticipants();
					for (Participant participant : participants) {
				        if (myId.equals(participant.getId())) {
				            isExist = true;
				            break;
				        }
				    }
					if(!isExist) {
						//输出已经不再集群中了哦 
						LOGGER.info(myId + " current does not exist on zk");	
			
						latch.close();					
						
						latch = new LeaderLatch(client, latchPath ,myId);
						latch.start();
						LOGGER.info(myId + " success reset leaderLatch @ " + latchPath);

					}
					
					
					//查看当前leader是否是自己
				    //注意，不能用leaderLatch.hasLeadership()因为有zk数据丢失的不确定性
				    //利用serverId对比确认是否主为自己
				    Participant leader = latch.getLeader();
				    boolean hashLeaderShip = myId.equals(leader.getId());				    
				    judgeIsLeader(hashLeaderShip);				    

				} catch (Exception e) {					
				    judgeIsLeader(false);    					
					e.printStackTrace();
				}			
				
			}
		};
	}
	//缓冲区 判断是否是leader
	public void judgeIsLeader(boolean hashLeaderShip) {
		//主从切换缓冲
	    if(hashLeaderShip) {
	        isLeaderCount++;
	        isSlaveCount = 0;
	    } else {
	        isLeaderCount = 0;
	        isSlaveCount ++;
	    }

		if (isLeaderCount > 3 && !isLeader) {
	        LOGGER.info(myId + " Currently run as leader");
			isLeader = true;
			//执行换为leader的方法
			//isLeaderRunnable.run();
			isLeaderRunnable();
		}

	    if (isSlaveCount > 3 && isLeader) {
	        LOGGER.info(myId + " Currently run as slave");
	    	isLeader = false;
	    	notLeaderRunnable();
	    }
	}
	
	//是否leader节点
	public boolean isLeaderShip(){
		return isSlaveCount == 0 && isLeader;
	}
	
	public Collection<Participant> getParticipants() throws Exception {
		return latch.getParticipants();
	}
	
	public int getParticipantsCount() {		
		try {
			return latch.getParticipants().size();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOGGER.error("get clusters number error");
			return 0;
		} 
	}
}
