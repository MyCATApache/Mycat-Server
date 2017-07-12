package io.mycat.sqlengine;

import io.mycat.MycatServer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 批量SQL 任务池
 */
public class BatchSQLJob {

    /**
     * 执行中任务列表
     */
	private ConcurrentHashMap<Integer, SQLJob> runningJobs = new ConcurrentHashMap<Integer, SQLJob>();
    /**
     * 待执行任务列表
     */
	private ConcurrentLinkedQueue<SQLJob> waitingJobs = new ConcurrentLinkedQueue<SQLJob>();
    /**
     * 无更多任务提交
     */
	private volatile boolean noMoreJobInput = false;

    /**
     * 提交任务
     * 若允许并发执行，则直接执行任务
     * 若不允许并行，则提交到待执行队列。若无正在执行中的任务，则从等待队列里获取任务进行执行。
     *
     * @param newJob 任务
     * @param parallExecute 是否并发执行
     */
    public void addJob(SQLJob newJob, boolean parallExecute) {
        if (parallExecute) {
            runJob(newJob);
        } else {
            waitingJobs.offer(newJob);
            if (runningJobs.isEmpty()) { // 若无正在执行中的任务，则从等待队列里获取任务进行执行。
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

    /**
     * 执行任务
     *
     * @param newJob 任务
     */
	private void runJob(SQLJob newJob) {
		// EngineCtx.LOGGER.info("run job " + newJob);
		runningJobs.put(newJob.getId(), newJob);
		MycatServer.getInstance().getBusinessExecutor().execute(newJob);
	}

    /**
     * 有个任务完成回调。若有未完成的任务，顺序执行一个未完成的任务
     *
     * @param sqlJob 任务
     * @return 任务是否全部完成 && 无心任务提交
     */
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
