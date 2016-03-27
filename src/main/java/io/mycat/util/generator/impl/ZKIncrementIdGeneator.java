package io.mycat.util.generator.impl;

import io.mycat.util.generator.IncrementIdGeneator;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @ClassName: ZKIncrementIdGeneator
 * @Description:zk实现
 * @author chenlinlin
 * @date 2016年3月26日 下午7:33:47
 */
public class ZKIncrementIdGeneator implements IncrementIdGeneator {

	// 单例实现
	private static class Holder {

		private static final ZKIncrementIdGeneator instance = new ZKIncrementIdGeneator();

	}

	private CuratorFramework client;

	private String connectionString;

	private String namespace;
	private static final String DEFAULT_NAMESPACE = "mycat_id";
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

	private volatile boolean started = false;

	private ZKIncrementIdGeneator(){

	}

	public static final ZKIncrementIdGeneator getInstance() {
		Holder.instance.init();
		return Holder.instance;
	}

	public ZKIncrementIdGeneator(String connectionString, String namespace){
		this.connectionString = connectionString;
		this.namespace = namespace;
	}

	private void init() {

		if (started) {
			return;
		}
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.builder().connectString(connectionString).retryPolicy(retryPolicy).namespace(DEFAULT_NAMESPACE).build();
		client.start();
		started = true;
	}

	private void destroy() {
		if (!started) {
			return;
		}
		if (client != null) {
			client.close();
			started = false;
		}
	}

	@Override
	public String generateId(String sequenceName) {

		String currentDate = dateFormat.format(new Date());
		String idPath = getIdPath(sequenceName, currentDate);
		try {

			String serialNodeCreated = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(idPath);

			// 鏍煎紡缁勬垚锛氭棩鏈搴忓垪鍙
			String incrementId = String.format("%s%s", currentDate, serialNodeCreated.replace(idPath, ""));

			return incrementId;
		} catch (Exception e) {
			throw new RuntimeException("generate error!" + e.getMessage());
		}
	}

	private String getIdPath(String sequenceName, String currentDate) {

		return String.format("/%s/increment_id/%s/%s/%s", this.namespace, sequenceName, currentDate, currentDate);
	}

	@Override
	public String generateIdWithPrefix(String sequenceName, String prefix) {
		String currentDate = dateFormat.format(new Date());
		String idPath = getIdPath(sequenceName, currentDate);
		try {

			String serialNodeCreated = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(idPath);

			// 鏍煎紡缁勬垚锛氭棩鏈搴忓垪鍙
			String incrementId = String.format("%s%s%s", prefix, currentDate, serialNodeCreated.replace(idPath, ""));

			return incrementId;
		} catch (Exception e) {
			throw new RuntimeException("generate error!" + e.getMessage());
		}
	}
}
