package io.mycat.server.quartz.job;

import java.io.Serializable;

import org.quartz.Job;

@SuppressWarnings("unchecked")
public class JobConfiguration implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * @param jobName 任务名
	 * @param targetClazz 定时执行类
	 * @param targetMethod 定时执行方法
	 * @param params 方法携带参数
	 * @param cron 表达式
	 * @param description 任务描述
	 */
	public JobConfiguration(String jobName, String targetClazz, String targetMethod, Object[] params, String cron,
			String description) {
		super();
		this.jobName = jobName;
		this.targetClazz = targetClazz;
		this.targetMethod = targetMethod;
		this.params = params;
		this.cron = cron;
		this.description = description;
	}

	public JobConfiguration() {
		super();
	}

	/**
	 * 作业名称.
	 */
	private String jobName;

	/**
	 * 作业实现类名称.
	 */
	private Class<? extends Job> jobClass = getJobClass();

	/**
	 * 目标执行类
	 */
	private String targetClazz;

	/**
	 * 目标执行方法
	 */
	private String targetMethod;

	/**
	 * 执行方法 所传递的参数
	 */

	private Object[] params;

	/**
	 * 作业启动时间的cron表达式.
	 */
	private String cron;

	/**
	 * 作业描述信息.
	 */
	private String description = "";

	/**
	 * 初始化任务执行类
	 */
	{
		jobClass = JobExecute.class;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getCron() {
		return cron;
	}

	public void setCron(String cron) {
		this.cron = cron;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Class<? extends Job> getJobClass() {
		if (this.jobClass == null) {
			return JobExecute.class;
		}
		return jobClass;
	}

	public void setJobClass(Class<? extends Job> jobClass) {
		this.jobClass = jobClass;
	}

	public String getTargetClazz() {
		return targetClazz;
	}

	public void setTargetClazz(String targetClazz) {
		this.targetClazz = targetClazz;
	}

	public String getTargetMethod() {
		return targetMethod;
	}

	public void setTargetMethod(String targetMethod) {
		this.targetMethod = targetMethod;
	}

	public Object[] getParams() {
		return params;
	}

	public void setParams(Object[] params) {
		this.params = params;
	}

}
