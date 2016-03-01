package io.mycat.sqlengine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.mycat.MycatServer;

public class BatchSQLJob {

	private ConcurrentHashMap<Integer, SQLJob> runningJobs = new ConcurrentHashMap<Integer, SQLJob>();
	private ConcurrentLinkedQueue<SQLJob> waitingJobs = new ConcurrentLinkedQueue<SQLJob>();
	private volatile boolean noMoreJobInput = false;

	public void addJob(SQLJob newJob, boolean parallExecute) {

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

	public void setNoMoreJobInput(boolean noMoreJobInput) {
		this.noMoreJobInput = noMoreJobInput;
	}

	private void runJob(SQLJob newJob) {
		// EngineCtx.LOGGER.info("run job " + newJob);
		runningJobs.put(newJob.getId(), newJob);
		MycatServer.getInstance().getBusinessExecutor().execute(newJob);
	}

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
