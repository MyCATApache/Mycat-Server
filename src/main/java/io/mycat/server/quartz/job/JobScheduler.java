package io.mycat.server.quartz.job;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import com.google.common.base.Joiner;

public class JobScheduler {

	private JobDetail jobDetail;

	private JobConfiguration jobConfiguration;

	private Scheduler scheduler;

	private Logger log = Logger.getLogger(JobScheduler.class);

	public JobScheduler() {
	}

	public JobScheduler(JobConfiguration jobConfiguration) {
		this.jobConfiguration = jobConfiguration;
	}

	/**
	 * 初始化作业.
	 * 
	 * @throws SchedulerException
	 */
	public void init() throws SchedulerException {
		jobDetail = createJobDetail();
		scheduler = initializeScheduler(jobDetail.getKey().toString());
		scheduleJob(createTrigger(jobConfiguration.getCron()));
	}

	/**
	 * 创建任务JOb
	 * 
	 * @return
	 */
	private JobDetail createJobDetail() {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put("jobConfiguration", jobConfiguration);
		JobDetail result = JobBuilder.newJob(jobConfiguration.getJobClass()).setJobData(jobDataMap)
				.withIdentity(jobConfiguration.getJobName()).build();
		return result;
	}

	/**
	 * 创建触发器
	 * 
	 * @param cronExpression
	 * @return
	 */
	private CronTrigger createTrigger(final String cronExpression) {
		CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);
		return TriggerBuilder.newTrigger().withIdentity(jobConfiguration.getJobName()).withSchedule(cronScheduleBuilder)
				.build();
	}

	/**
	 * 初始化Scheduler
	 * 
	 * @param jobName
	 * @return
	 * @throws SchedulerException
	 */
	private Scheduler initializeScheduler(final String jobName) throws SchedulerException {
		Scheduler schedule = StdSchedulerFactory.getDefaultScheduler();
		schedule.getListenerManager().addTriggerListener(new JobTriggerListener());
		return schedule;
	}

	/**
	 * 获取 QRTZ Scheduler配置
	 * 
	 * @param jobName
	 * @return
	 */
	private Properties getBaseQuartzProperties(final String jobName) {
		Properties result = new Properties();
		result.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
		result.put("org.quartz.threadPool.threadCount", "1");
		result.put("org.quartz.scheduler.instanceName", jobName);
		prepareEnvironments(result);
		return result;
	}

	protected void prepareEnvironments(final Properties props) {
	}

	private void scheduleJob(final CronTrigger trigger) throws SchedulerException {
		if (!scheduler.checkExists(jobDetail.getKey())) {
			scheduler.scheduleJob(jobDetail, trigger);
		}
		scheduler.start();
	}

	/**
	 * 获取下次作业触发时间.
	 * 
	 * @return 下次作业触发时间
	 */
	public Date getNextFireTime() {
		Date result = null;
		List<? extends Trigger> triggers;
		try {
			triggers = scheduler.getTriggersOfJob(jobDetail.getKey());
		} catch (final SchedulerException ex) {
			return result;
		}
		for (Trigger each : triggers) {
			Date nextFireTime = each.getNextFireTime();
			if (null == nextFireTime) {
				continue;
			}
			if (null == result) {
				result = nextFireTime;
			} else if (nextFireTime.getTime() < result.getTime()) {
				result = nextFireTime;
			}
		}
		return result;
	}

	/**
	 * 停止作业.
	 * 
	 * @throws JobExecutionException
	 */
	public void stopJob() throws JobExecutionException {
		try {
			scheduler.pauseAll();
		} catch (final SchedulerException ex) {
			throw new JobExecutionException(ex);
		}
	}

	/**
	 * 恢复手工停止的作业.
	 * 
	 * @throws SchedulerException
	 */
	public void resumeManualStopedJob() throws SchedulerException {
		scheduler.resumeAll();
	}

	/**
	 * 立刻启动作业.
	 * 
	 * @throws SchedulerException
	 */
	public void triggerJob() throws SchedulerException {
		scheduler.triggerJob(jobDetail.getKey());
	}

	/**
	 * 关闭调度器.
	 * 
	 * @throws SchedulerException
	 */
	public void shutdown() throws SchedulerException {
		scheduler.shutdown();
	}

	/**
	 * 重新调度作业.
	 * 
	 * @throws SchedulerException
	 */
	public void rescheduleJob(final String cronExpression) throws SchedulerException {
		scheduler.rescheduleJob(TriggerKey.triggerKey(jobConfiguration.getJobName()), createTrigger(cronExpression));
	}
}
