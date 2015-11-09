/** 
 * @Title:job计划管理类，负责初始化curator，启动监听Leader Selecter线程  
 * @Desription:job计划管理类，负责初始化curator，启动监听Leader Selecter线程
 * @Company:CSN
 * @ClassName:ScheduleManager.java
 * @Author:Justic
 * @CreateDate:2015-6-30   
 * @UpdateUser:Justic  
 * @Version:0.1 
 *    
 */

package io.mycat.server.quartz.job;

import com.alibaba.fastjson.JSON;

import io.mycat.server.quartz.zk.ZkConfig;
import io.mycat.server.quartz.zk.ZkService;

/**
 * @ClassName: ScheduleManager
 * @Description: job计划管理类，负责初始化curator，启动监听Leader Selecter线程
 * @author: Justic
 * @date: 2015-6-30
 * 
 */
public class JobService {

	private ZkService zkService;
	private ZkConfig zkConfig;
	private String rootPath;
	// 用于标记是否已初始化
	private String nodePath;

	public JobService(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
		this.nodePath = zkConfig.getRootPath() + "/" + zkConfig.getNodeName();
		this.rootPath = zkConfig.getRootPath();
		zkService = new ZkService(zkConfig);
	}

	public void addJob(JobConfiguration job) throws Exception {
		zkService.createNode(nodePath + "/" + job.getJobName(), JSON.toJSONString(job).getBytes("utf-8"));
	}

}
