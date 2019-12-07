package io.mycat.sqlengine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.mycat.MycatServer;
import io.mycat.config.model.SystemConfig;

public class BatchSQLJob {

	private ConcurrentHashMap<Integer, SQLJob> runningJobs = new ConcurrentHashMap<Integer, SQLJob>();
	private ConcurrentLinkedQueue<SQLJob> waitingJobs = new ConcurrentLinkedQueue<SQLJob>();
	private volatile boolean noMoreJobInput = false;
	/*
	 * 
	 * parallExecute: 是否可以并行执行
	 * */
	public void addJob(SQLJob newJob, boolean parallExecute) {
        SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
        parallExecute = parallExecute && (system.getParallExecute() == 1);
        if (parallExecute) {
			runJob(newJob);
		} else {
			waitingJobs.offer(newJob);
			if (runningJobs.isEmpty()) {
				SQLJob job = waitingJobs.poll();
				if (job != null) {
					runJob(job);
				}
			}
		}
	}
	//设置批量任务已经不会在添加任务了。
	public void setNoMoreJobInput(boolean noMoreJobInput) {
		this.noMoreJobInput = noMoreJobInput;
	}
	//执行任务
	private void runJob(SQLJob newJob) {
		// EngineCtx.LOGGER.info("run job " + newJob);
		runningJobs.put(newJob.getId(), newJob);
		MycatServer.getInstance().getBusinessExecutor().execute(newJob);
	}
	//单个的任务执行完毕。 等待任务列表中有任务，  继续执行下一个任务。	
	//返回： 是否所有的任务执行完毕。	
	public boolean jobFinished(SQLJob sqlJob) {
		if (EngineCtx.LOGGER.isDebugEnabled()) {
			EngineCtx.LOGGER.info("job finished " + sqlJob);
		}
		runningJobs.remove(sqlJob.getId());
		SQLJob job = waitingJobs.poll();
		if (job != null) {
			runJob(job);
			return false;
		} else {
			if (noMoreJobInput) {
				return runningJobs.isEmpty() && waitingJobs.isEmpty();
			} else {
				return false;
			}
		}

	}
}
