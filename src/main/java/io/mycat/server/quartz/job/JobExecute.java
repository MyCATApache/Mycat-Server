package io.mycat.server.quartz.job;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import io.mycat.server.quartz.invoke.QrtzMethodInvoker;

public class JobExecute implements Job {

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
		JobConfiguration jobConfiguration = (JobConfiguration) jobDataMap.get("jobConfiguration");
		String clazzName = jobConfiguration.getTargetClazz();
		String methodName = jobConfiguration.getTargetMethod();
		Object[] params = jobConfiguration.getParams();
		boolean invoke = QrtzMethodInvoker.invoke(clazzName, methodName, params);
		// 如果执行不成功 抛出异常!
		if (!invoke) {
			throw new JobExecutionException();
		}
	}

}
