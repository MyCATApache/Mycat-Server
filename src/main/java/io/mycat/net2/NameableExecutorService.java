package io.mycat.net2;

import java.util.concurrent.ExecutorService;

/**
 * @author wuzh
 */
public interface NameableExecutorService extends ExecutorService {

	public String getName();
}