/** 
 * @Title:监听节点变化，新增or删除 
 * @Desription:监听节点变化，新增or删除
 * @Company:CSN
 * @ClassName:NodePathCacheListener.java
 * @Author:Justic
 * @CreateDate:2015-6-30   
 * @UpdateUser:Justic  
 * @Version:0.1 
 *    
 */

package io.mycat.server.quartz.zk.listen;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import com.alibaba.fastjson.JSON;

import io.mycat.server.quartz.job.JobConfiguration;
import io.mycat.server.quartz.job.JobScheduler;

public class NodePathCacheListener implements PathChildrenCacheListener {

	/**
	 * 事件捕捉
	 * 
	 * @param client
	 * @param event
	 * @throws Exception
	 * @see org.apache.curator.framework.recipes.cache.PathChildrenCacheListener#childEvent(org.apache.curator.framework.CuratorFramework,
	 *      org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent)
	 */

	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		if (event.getType().equals(Type.CHILD_ADDED)) {

		}

		if (event.getData() == null) {
			return;
		}
		byte[] data = event.getData().getData();
		if (data == null) {
			return;
		}
		String str = new String(data, "utf-8");
		boolean isJson = isJson(str);
		if (!isJson) {
			return;
		}

		JobConfiguration jobConfiguration = JSON.parseObject(str, JobConfiguration.class);
		if (event.getType().equals(Type.CHILD_REMOVED)) {
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
			JobKey jobKey = new JobKey(jobConfiguration.getJobName());
			scheduler.deleteJob(jobKey);
		}

		if (event.getType().equals(Type.CHILD_ADDED)) {
			// 初始化并启动任务
			new JobScheduler(jobConfiguration).init();
		}
		
		if (event.getType().equals(Type.CHILD_UPDATED)) {
			Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
			JobKey jobKey = new JobKey(jobConfiguration.getJobName());
			scheduler.deleteJob(jobKey);
			new JobScheduler(jobConfiguration).init();
		}
	}

	public boolean isJson(String str) {
		try {
			if (str == null || str.equals("")) {
				return false;
			}
			JSON.parseObject(str);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

}
