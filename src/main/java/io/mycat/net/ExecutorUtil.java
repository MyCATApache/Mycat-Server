package io.mycat.net;

import java.util.concurrent.LinkedTransferQueue;

/**
 * 生成一个有名字的（Nameable）Executor，容易进行跟踪和监控
 * 
 * @author wuzh
 */
public class ExecutorUtil {

	public static final NameableExecutor create(String name, int size) {
		NameableThreadFactory factory = new NameableThreadFactory(name, true);
		return new NameableExecutor(name, size,
				new LinkedTransferQueue<Runnable>(), factory);
	}

	public static final NamebleScheduledExecutor createSheduledExecute(
			String name, int size) {
		NameableThreadFactory factory = new NameableThreadFactory(name, true);
		return new NamebleScheduledExecutor(name, size, factory);
	}

}