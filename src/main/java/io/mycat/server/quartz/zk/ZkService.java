package io.mycat.server.quartz.zk;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

import com.google.common.base.Preconditions;

import io.mycat.server.quartz.zk.listen.NodePathCacheListener;

public class ZkService {

	private CuratorFramework client;
	private ZkConfig zkConfig;

	public ZkService(ZkConfig zkConfig) {
		this.zkConfig = zkConfig;
		init();
	}

	public void createNode(String nodePath, String value) throws Exception {
		createNode(nodePath, value.getBytes());
	}

	public void createNode(String nodePath, byte[] value) throws Exception {
		client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodePath, value);
	}

	public String getDirectly(final String key) {
		try {
			return new String(client.getData().forPath(key), Charset.forName("UTF-8"));
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
			return null;
		}
	}

	public List<String> getChildrenKeys(final String key) {
		try {
			return client.getChildren().forPath(key);
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
			return Collections.emptyList();
		}
	}

	public boolean isExisted(final String key) {
		try {
			return null != client.checkExists().forPath(key);
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
			return false;
		}
	}

	public void persist(final String key, final String value) {
		try {
			if (!isExisted(key)) {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(key,
						value.getBytes());
			} else {
				update(key, value);
			}
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
		}
	}

	public void update(final String key, final String value) {
		try {
			client.inTransaction().check().forPath(key).and().setData()
					.forPath(key, value.getBytes(Charset.forName("UTF-8"))).and().commit();
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
		}
	}

	public void persistEphemeral(final String key, final String value) {
		try {
			if (isExisted(key)) {
				client.delete().deletingChildrenIfNeeded().forPath(key);
			}
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(key,
					value.getBytes(Charset.forName("UTF-8")));
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
		}
	}

	public void persistEphemeralSequential(final String key) {
		try {
			client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(key);
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
		}
	}

	public void remove(final String key) {
		try {
			client.delete().deletingChildrenIfNeeded().forPath(key);
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
		}
	}

	public long getRegistryCenterTime(final String key) {
		long result = 0L;
		try {
			String path = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
					.forPath(key);
			result = client.checkExists().forPath(path).getCtime();
			// CHECKSTYLE:OFF
		} catch (final Exception ex) {
			// CHECKSTYLE:ON
		}
		Preconditions.checkState(0L != result, "Cannot get registry center time.");
		return result;
	}

	public void init() {
		try {
			this.client = buildZk();
			addListener(client, zkConfig.getRootPath()+"/"+zkConfig.getNodeName(), new NodePathCacheListener());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public CuratorFramework buildZk() throws Exception {
		CuratorFramework zk = CuratorFrameworkFactory.builder().connectString(zkConfig.getZkServer())
				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, zkConfig.getRetryNTimes()))
				.connectionTimeoutMs(zkConfig.getConnectionTimeoutMs()).build();
		zk.start();
		return zk;
	}

	/**
	 * 注册zookeeper 节点监听器
	 * 
	 * @param zkClient
	 * @param path
	 */
	public PathChildrenCache addListener(CuratorFramework zkClient, String path, PathChildrenCacheListener pis) {
		PathChildrenCache cache = null;
		try {
			cache = new PathChildrenCache(zkClient, path, true);
			cache.start();
		} catch (Exception e) {
			try {
				cache.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}

		cache.getListenable().addListener(pis);
		return cache;
	}
}
