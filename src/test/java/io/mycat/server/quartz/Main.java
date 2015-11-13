package io.mycat.server.quartz;

import io.mycat.server.quartz.job.JobConfiguration;
import io.mycat.server.quartz.job.JobService;
import io.mycat.server.quartz.zk.ZkConfig;

public class Main {

	public static void main(String[] args) throws Exception {

		//初始化 zk
		ZkConfig zkConfig = new ZkConfig("127.0.0.1", "/mycat/quartz", "node1", 6000, 15000);
		JobService jobService = new JobService(zkConfig);
		JobConfiguration job = new JobConfiguration("test14", "io.mycat.server.quartz.TestClass", "test", null,
				"*/5 * * * * ?", "test");
		jobService.addJob(job);
		System.in.read();
	}
}
