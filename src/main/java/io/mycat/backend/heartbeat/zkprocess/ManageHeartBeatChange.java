package io.mycat.backend.heartbeat.zkprocess;

import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.util.StringUtil;
import io.mycat.util.TimeUtil;
import io.mycat.util.ZKUtils;
import io.netty.util.internal.ConcurrentSet;

//作为leader节点去管理所有的
//注释：
/* 源文件名：ManageHeartBeatChange.java
* 文件版本：1.0.0
* 创建作者：zwy
* 创建日期：2018年5月20日
*/
public class ManageHeartBeatChange implements Runnable {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(ManageHeartBeatChange.class);
	
	public static int ENTER_SELECT = 0; //每个人进行 投票
	public static int IS_SELECT = 1; //开始进入统计票数
	public static int IS_CHANGING = 2; //正在切换节点.

	public static int NOT_SELECT = -1;
	private final String dataHost;
	private volatile AtomicInteger statue = new AtomicInteger(NOT_SELECT);
	public ConcurrentSet<String> voteSet = new ConcurrentSet<>(); //投票的结果集
	private volatile PathChildrenCache manageVoteCache; //投票的结果集处理
	private final CuratorFramework client;
	private final String path; 
	final ScheduledExecutorService service = MycatServer.getInstance().getHeartbeatScheduler(); //定时器
	private volatile NodeCache changingResultNode; //节点切换读写节点的状态的改变 的处理 
	private final MycatLeaderLatch mycatLeaderLatch; // 
	private  InterProcessMutex changingStatueLock;
	private final String manageVotePath; 
	private final String changingResultPath; //节点切换读写节点的状态的改变的路径.

	private long maxTimeToWait = 60 * 1000; //最多的等待时间去进行投票结果
	private final long minTimeToSwitched = 30 * 60 * 1000; //至少的等待时间去进行下一次切换
	private volatile long changingFinishDate = 0; //节点切换读写节点的状态的改变的路径.
	private volatile ScheduledFuture<?> future = null;
	private volatile ScheduledFuture<?> changingResultFutrue = null;
	public ManageHeartBeatChange(MycatLeaderLatch myLeaderLatch,
			final String dataHost) throws Exception{
		statue.set(NOT_SELECT);
		this.dataHost = dataHost; //dataSource的名称
		this.path = ZKUtils.getZKBasePath() +"heartbeat/" + dataHost +"/";
		this.manageVotePath =  path+ "voteInformation";
		this.changingResultPath = path + "changingStatue";
		this.client = ZKUtils.getConnection();
		changingStatueLock =  new InterProcessMutex(client, ZKUtils.getZKBasePath() +"heartbeat/changingStatueLock");
		this.mycatLeaderLatch = myLeaderLatch;		
		
	}
	//收集投票结果
	public boolean addPath(String nodePath) {
		LOGGER.debug("add vote information "  + nodePath);
		//判断是否可以收集投票结果 如果不行直接删除
		if(TimeUtil.currentTimeMillis() - changingFinishDate  < minTimeToSwitched && statue.get() == NOT_SELECT ) {
			try {
				client.delete().deletingChildrenIfNeeded().forPath(nodePath);
			} catch (Exception e) {
				e.printStackTrace(); 
				LOGGER.error("remove vote information debug during not voting time" ,e);
			}
			return false;
		}
		if(statue.compareAndSet(NOT_SELECT, ENTER_SELECT)){
//			beginVoteTime = new Date(); // 开始投票时间
			 future = service.schedule(this, maxTimeToWait , TimeUnit.MILLISECONDS);
		}
		if(statue.get() == ENTER_SELECT) {
			boolean flag = voteSet.add(nodePath); 		
			return flag;
		}
		
		return false;		
	}
	
	//清除投票结果
	public boolean removePath(String nodePath) {
		LOGGER.debug("remove vote Information" + nodePath);
		 //删除投票结果.		
		if(statue.get() == IS_CHANGING) {
			boolean flag =  voteSet.remove(nodePath);
			if(voteSet.isEmpty() && changingResultFutrue == null){
				statue.set(NOT_SELECT);
			}	
			return flag;
		}				
		return false;
	}
	//如果是leader 节点 开始进行监听
	public void leaderlisten() {
		try {
			if(manageVoteCache != null){
				manageVoteCache.close();
			}
			manageVoteCache = new PathChildrenCache(client, manageVotePath, true);
			final ManageHeartBeatChange manager = this;
			//监听投票结果, 然后决定需要选举哪一个为最终的结果。
			manageVoteCache.getListenable().addListener(new PathChildrenCacheListener() {			
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)  {
					// TODO Auto-generated method stub
					LOGGER.debug("event Type " + event.getType());
//					LOGGER.debug( ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID) +" is leader ? " + mycatLeaderLatch.isLeaderShip());
					if(null != event.getData()) {
						Type type = event.getType();
						switch(type) {
							case CHILD_ADDED : {							
							}
							case CHILD_UPDATED: {
								manager.addPath(event.getData().getPath());
								break;
							}
							case CHILD_REMOVED:{
								manager.removePath(event.getData().getPath());
								break;
							}
							default:
								break;						
						}
						if(manager.hasCollectFinish()) {
							manager.run();
						}
					}
				
				}
			});
			manageVoteCache.start();
			if(changingResultNode != null){
				changingResultNode.close();
			}
			//监听切换结果,如果全部完成写入完成的时间

			changingResultNode =  new NodeCache( client,  changingResultPath);		
			changingResultNode.start();		
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}

	}
	//如果不是leader 节点 停止进行监听
	public void stop() {		
		try {
			manageVoteCache.close();
			manageVoteCache = null;
			changingResultNode.close();
			changingResultNode = null;
			//isLeader.compareAndSet(true, false);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
	}
	/*
	 * 决定哪个节点为最终的投票结果
	 * 所有的节点投票完成 或者 5分钟之内有投票的
	 * */
	@Override
	public void run() {
		if(!statue.compareAndSet(ENTER_SELECT, IS_SELECT) || getNodeSize() == 0) {
			return;
		}
		if(future != null) {
			future.cancel(false);
			future = null;
		}
		//获取最后一次的投票时间 如果
		List<ChildData> ChildDataList = manageVoteCache.getCurrentData();
		Map<Integer, Integer> countMap = new HashMap<>();
		Integer maxIndex = -1 ;
		Integer maxCount= -1 ;
		try {
		    Collection<Participant> participants = mycatLeaderLatch.getParticipants();
			for(ChildData childData : ChildDataList) {
				String data = new String(childData.getData());
				LOGGER.debug(childData.getPath()+ "  " + data);
				int index = data.indexOf("=");
				Integer key =  Integer.valueOf(data.substring(index + 1));
				String myId = data.substring(0, index);
				//只对在线的节点进行统计 如果某个节点挂了 不再进行统计了
				boolean checkExist = false;
				for(Participant participant : participants) {
					if(participant.getId().equals(myId)) {
						checkExist = true;
						break;
					}
				}
				if(!checkExist){
					continue;
				}
				
				Integer value = countMap.get(key);
				
				if(value == null) {
					value = new Integer(0);
				}
				value += 1;
				countMap.put(key, value);
				//所有总数最大的为投票结果
				if(maxCount.compareTo(value) < 0) {
					maxCount = value;
					maxIndex = key;
				}			
			}
			//节点切换
			statue.set(IS_CHANGING);
		
			if(maxIndex != -1) { 
				LOGGER.debug("投票结果：" + dataHost + " = " + maxIndex);
				//向集群写入修改的结果
				ZKUtils.createPath(changingResultPath, "");
				boolean result = MycatServer.getInstance().saveDataHostIndexToZk(dataHost, maxIndex);
				if(result) {
					//开启对结果切换的监控
					startChangingResultListen();
				} else {
					 //删除投票结果。
					try {				
						for(ChildData childData : ChildDataList)  {
							client.delete().deletingChildrenIfNeeded().forPath(childData.getPath());
						}
					} catch (Exception e) {
						LOGGER.error(e.getMessage());
						e.printStackTrace();
					};
				}
				
			} else {
				LOGGER.debug("投票错误：" + dataHost + " = " + maxIndex);				
			}

			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}

	}
	//对状态结果切换的主动监控
	private void startChangingResultListen() {
		 //主动监控切换节点的状态
		 changingResultFutrue = service.scheduleAtFixedRate(new Runnable() {				
			@Override
			public void run() {
				ChildData currentData = changingResultNode.getCurrentData();
				if(null != currentData) {
					try{
						byte[] data = changingResultNode.getCurrentData().getData();
						Properties properties = new Properties();
						properties.load(new ByteArrayInputStream(data));
						int count = 0;
						Collection<Participant> participants = mycatLeaderLatch.getParticipants();
						for(Participant participant : participants) {
							String key = participant.getId() + "_endTime";
							String value = properties.getProperty(key);
							if(!StringUtil.isEmpty(value)) {
								count ++;								
							}else {
								LOGGER.debug(String.format("%s 还未结束切换", participant.getId()));
							}
						}
						
						String changingFinishKey = dataHost + "_changing_finish_time";
						String value = properties.getProperty(changingFinishKey);
						if(!StringUtil.isEmpty(value)) {
							changingFinishDate = Long.valueOf(value);
						}						
						int onLineNode = participants.size(); //在线的节点						
						if(count == onLineNode ) {							 //
							 LOGGER.debug("所有节点切换完成 ,当前时间" + TimeUtil.currentTimeMillis());
							 Map<String,String> propertyMap = new HashMap<>();
							 propertyMap.put(changingFinishKey, TimeUtil.currentTimeMillis()+"");
							 try{
								 changingStatueLock.acquire(30, TimeUnit.SECONDS);
								 ZKUtils.writeProperty( changingResultPath, propertyMap);
								 if(changingResultFutrue !=null) {
									 changingResultFutrue.cancel(false);
									 changingResultFutrue = null;
								 }
								 
								 //删除投票结果。
								List<ChildData> ChildDataList = manageVoteCache.getCurrentData();
								for(ChildData childData : ChildDataList)  {
									client.delete().deletingChildrenIfNeeded().forPath(childData.getPath());
								}
							
								 if(voteSet.isEmpty() ){
									 //删除投票结果。
									statue.set(NOT_SELECT);
								 }	
								 
						  	 }finally {
						  		 changingStatueLock.release();
						  	  }
						}
					} catch (Exception e) {
						LOGGER.error(e.getMessage());
						e.printStackTrace();
					}
				} else {
					LOGGER.debug("集群切换结果的状态文件夹 已经被删除！！！");
				}					
				
			}
		}, 100, 1000, TimeUnit.MILLISECONDS);
	}

	public int getNodeSize(){
		return voteSet.size();
	}

	//所有节点收集完毕.
	public boolean hasCollectFinish() {
		return voteSet.size() == mycatLeaderLatch.getParticipantsCount();
	}	

}
