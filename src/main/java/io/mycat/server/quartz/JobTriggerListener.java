package io.mycat.server.quartz;

import java.util.Date;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.listeners.TriggerListenerSupport;

public class JobTriggerListener extends TriggerListenerSupport {

	private Logger logger = Logger.getLogger(JobTriggerListener.class);

	@Override
	public String getName() {
		return "JobTriggerListener";
	}

	// 监听任务是否完成
	@Override
	public void triggerComplete(Trigger trigger, JobExecutionContext context,
			CompletedExecutionInstruction triggerInstructionCode) {
		// 任务key
		JobKey jobKey = trigger.getJobKey();

		logger.info(jobKey + "on " + " was finish!");
		System.out.println(JobTriggerListener.class.getName()+"  正在执行!!!");
		super.triggerComplete(trigger, context, triggerInstructionCode);
	}

}
